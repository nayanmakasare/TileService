package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
	"github.com/micro/go-micro"
	"github.com/micro/go-micro/util/log"
	TileService "github.com/nayanmakasare/TileService/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)


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

var defaultLanguages = []string{"English",
	"Flemish",
	"French",
	"German",
	"Gujarati",
	"Hebrew",
	"Hindi",
	"Japanese",
	"Kannada",
	"Khmer",
	"Korean",
	"Malayalam",
	"Mandarin",
	"Marathi",
	"Norwegian",
	"Persian",
	"Portuguese",
	"Punjabi",
	"Russian",
	"Sanskrit",
	"Spanish",
	"Tamil",
	"Telugu",
}

type TileServiceHandler struct {
	MongoCollection *mongo.Collection
	RedisConnection *redis.Client
	EventPublisher  micro.Publisher
}

func (h *TileServiceHandler) SegregatingTilesAccordingToValues(redisKey, contentKey string, contentValue string) {
	myStages := mongo.Pipeline{
		//stage 1
		bson.D{{"$match", bson.D{{contentKey, bson.D{{"$in", bson.A{contentValue}}}}}}},
		//stage 2
		bson.D{{"$sort", bson.D{{"created_at", -1}}}},
		//stage 4
		bson.D{{"$project",
			bson.D{{"_id", 0},
				{"ref_id", 1}}}},
	}

	cur, err := h.MongoCollection.Aggregate(context.Background(), myStages, options.Aggregate().SetMaxTime(2000*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	for cur.Next(context.Background()) {
		h.RedisConnection.SAdd(redisKey, cur.Current.Lookup("ref_id").StringValue())
	}
	err = cur.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func (h *TileServiceHandler) CheckInRedis(redisKey string) (bool, error) {
	intCmdResult := h.RedisConnection.Exists(redisKey)
	if intCmdResult.Val() == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func (h *TileServiceHandler) GetMovieTiles(ctx context.Context, req *TileService.RowId, stream TileService.TileService_GetMovieTilesStream) error {
	var nextCursor uint64
	log.Info("Get FullData ", req.GetFullData)
	for {
		result, serverCursor, err := h.RedisConnection.SScan(req.RowId, nextCursor, "", 300).Result()
		if err != nil {
			return err
		}
		nextCursor = serverCursor
		counter := 0;
		for _, k := range h.RedisConnection.HMGet("cloudwalkerTiles", result...).Val() {
			var movieTile TileService.MovieTile
			//Important lesson, challenge was to convert interface{} to byte. used  ([]byte(k.(string)))
			err = proto.Unmarshal(([]byte(k.(string))), &movieTile)
			if err != nil {
				return err
			}
			counter++
			err = stream.Send(&movieTile)
			if err != nil {
				return nil
			}
		}
		log.Info(req.RowId," ",counter)
		if serverCursor == 0 {
			break
		}
	}
	return stream.Close()
}

func GetBytes(key interface{}) ([]byte) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(key)
	return buf.Bytes()
}

func (h *TileServiceHandler) GetRows(ctx context.Context, request *TileService.GetRowsRequest, stream TileService.TileService_GetRowsStream) error {
	if len(request.UserId) > 0 {
		return errors.New("Yet To implement")
	} else {
		for i, v := range defaultRows {
			tempRowSpecs := &TileService.RowSpec{
				RowName:     v,
				RowId:       v,
				RowShape:    "landscape",
				RowPosition: int32(i),
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

func (h *TileServiceHandler) InitializingEngine(ctx context.Context, req *TileService.InitializingEngineRequest, res *TileService.InitializingEngineResponse) error {
	log.Info("Triggered init")
	initProcessResult := make(chan bool, 1)
	go h.StoringAllTilesToRedis(initProcessResult)
	for _,k := range defaultLanguages {
		go h.SegregatingTilesAccordingToValues(MakeRedisKey("cloudwalker:languages:"+k), "metadata.languages", k)
	}
	res.IsDone = <-initProcessResult
	return nil
}

func (h *TileServiceHandler) StoringAllTilesToRedis(result chan bool) {
	myStages := mongo.Pipeline{
		bson.D{{"$project", bson.D{{"_id", 0}, {"ref_id", 1},
			{"metadata.title", 1},
			{"posters.landscape", 1},
			{"posters.portrait", 1},
			{"content.package", 1},
			{"content.detailPage", 1},
			{"content.target", 1},
			{"content.type", 1}}}},
	}
	cur, err := h.MongoCollection.Aggregate(context.Background(), myStages)
	if err != nil {
		result <- false
		log.Fatal(err)
	}
	for cur.Next(context.Background()) {
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
		h.RedisConnection.HSet("cloudwalkerTiles", cur.Current.Lookup("ref_id").StringValue(), resultByteArray)
	}
	err = cur.Close(context.TODO())
	if err != nil {
		result <- false
		log.Fatal(err)
	}
	result <- true
}
















//func (h *TileServiceHandler) StoringDataFromDbToRedis(redisKey, contentKey, contentValue string, resultStatus chan bool) {
//	log.Info("StoringDataFromDbToRedis 1")
//	myStages := mongo.Pipeline{
//		//stage 1
//		bson.D{{"$match", bson.D{{contentKey, bson.D{{"$in", bson.A{contentValue}}}}}}},
//		//stage 2
//		bson.D{{"$sort", bson.D{{"created_at", -1}}}},
//		//stage 3
//		bson.D{{"$limit", 200}},
//		//stage 4
//		//stage 4
//		bson.D{{"$project",
//			bson.D{{"_id", 0},
//				{"ref_id", 1},
//				{"metadata.title", 1},
//				{"posters.landscape", 1},
//				{"posters.portrait", 1},
//				{"content.package", 1},
//				{"content.detailPage", 1},
//				{"content.target", 1},
//				{"content.source", 1}}}},
//	}
//
//	cur, err := h.MongoCollection.Aggregate(context.Background(), myStages, options.Aggregate().SetMaxTime(2000*time.Millisecond))
//	if err != nil {
//		log.Info("StoringDataFromDbToRedis 2")
//		resultStatus <- false
//		log.Fatal(err)
//	}
//	for cur.Next(context.Background()) {
//		var movieTile TileService.MovieTile
//		err := cur.Decode(&movieTile)
//		movieTile.RefId = cur.Current.Lookup("ref_id").StringValue()
//		resultByteArray, err := proto.Marshal(&movieTile)
//		if err != nil {
//			log.Info("StoringDataFromDbToRedis 2")
//			resultStatus <- false
//			log.Fatal(err)
//		}
//		h.RedisConnection.SAdd(redisKey, resultByteArray)
//	}
//	err = cur.Close(context.Background())
//	if err != nil {
//		log.Info("StoringDataFromDbToRedis 3")
//		resultStatus <- false
//		log.Fatal(err)
//	}
//	resultStatus <- true
//}

//go h.StoringDataFromDbToRedis("cloudwalkerTrailers", "metadata.categories", "Movie Trailers", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerLatestMovies", "metadata.categories", "Movies", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerLatestSeries", "metadata.categories", "Series", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerLatestDocumentary", "metadata.categories", "Documentary", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerPopularVideos", "metadata.categories", "Popular Videos", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerCookery", "metadata.categories", "Cookery", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerHindiMusic", "metadata.categories", "Music Videos Hindi", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerEnglishMusic", "metadata.categories", "Music Videos English", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerNews", "metadata.categories", "News", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerApps", "metadata.categories", "App", initProcessResult)
//go h.StoringDataFromDbToRedis("cloudwalkerShortFilms", "metadata.categories", "Short Film", initProcessResult)

//TODO trial 1
//exists, err := h.CheckInRedis(req.RowId)
//if err != nil {
//return err
//}
//if exists {
//stringResultCmd := h.RedisConnection.SMembers(req.RowId)
//resultStringArray := stringResultCmd.Val()
//for i := 0 ; i < len(resultStringArray) ; i++ {
//var resultMovie TileService.MovieTile
//err := proto.Unmarshal([]byte(resultStringArray[i]), &resultMovie)
//if err != nil {
//return err
//}
//log.Info(resultMovie.Metadata.Title)
//err = stream.Send(&resultMovie)
//if err != nil {
//return err
//}
//}
//return stream.Close()
//} else {
//return errors.New("Data not in redis")
//}

//TODO trial 2 Finalized for Use
//exists, err := h.CheckInRedis(req.RowId)
//if err != nil {
//return err
//}
//if exists {
//var nextCursor uint64
//for {
//log.Info("Trigger 2")
//resultSet, serverCursor, err := h.RedisConnection.SScan(req.RowId, nextCursor, "", 20 ).Result()
//if err != nil {
//log.Info("Trigger 3")
//return err
//}
//nextCursor = serverCursor
//log.Info("Trigger 4 ",nextCursor)
//for _,v := range resultSet {
//var resultMovie TileService.MovieTile
//err := proto.Unmarshal([]byte(v), &resultMovie)
//if err != nil {
//return err
//}
//err = stream.Send(&resultMovie)
//if err != nil {
//return err
//}
//}
//if serverCursor == 0 {
//stream.Close()
//break
//}
//}
//return nil
//} else {
//return errors.New("Data not in redis")
//}

//TODO Trial 3
//exists, err := h.CheckInRedis(req.RowId)
//if err != nil {
//return err
//}
//if exists {
//var nextCursor uint64
//for {
//resultSet, serverCursor, err := h.RedisConnection.SScan(req.RowId, nextCursor, "", 20 ).Result()
//if err != nil {
//return err
//}
//nextCursor = serverCursor
//for _,v := range resultSet {
//var resultMovie TileService.MovieTile
//err := proto.Unmarshal([]byte(v), &resultMovie)
//if err != nil {
//return err
//}
//err = stream.Send(&resultMovie)
//if err != nil {
//return err
//}
//}
//if serverCursor == 0 {
//err := stream.Close()
//if err != nil {
//return err
//}
//break
//}
//}
//return nil
//} else {
//return errors.New("Data not in redis")
//}

//TODO Trial 4
//func (h *TileServiceHandler) GetMovieTiles(ctx context.Context, req *TileService.RowId, stream TileService.TileService_GetMovieTilesStream) error {
//	exists, err := h.CheckInRedis(req.RowId)
//	if err != nil {
//		return err
//	}
//	if exists {
//		cancleContext, cancle := context.WithCancel(ctx)
//		defer cancle()
//		movieChan, errorChan := h.FetchMovieTiles(cancleContext, req.RowId)
//		var wg sync.WaitGroup
//
//		//starting 5 worker Threads
//		for i := 0; i < 5; i++ {
//			wg.Add(1)
//			go func() {
//				defer wg.Done()
//				for {
//					select {
//					case err, ok := <-errorChan:
//						if !ok {
//							return
//						}
//						if err == context.Canceled {
//							return
//						}
//						log.Fatal(err)
//					case movieTiles, ok := <-movieChan:
//						if !ok {
//							stream.Close()
//							cancle()
//							return
//						}
//						stream.Send(&movieTiles)
//						log.Info(movieTiles.Metadata.Title)
//					}
//				}
//			}()
//		}
//		wg.Wait()
//		return nil
//	} else {
//		return errors.New("Data not in redis")
//	}
//}

//func (h *TileServiceHandler) FetchMovieTiles(ctx context.Context, redisKey string) (<- chan TileService.MovieTile, <- chan error)  {
//	movieChan := make(chan TileService.MovieTile, 100)
//	errorChan := make(chan error, 1)
//	go h.AsyncFetch(ctx, movieChan, errorChan, redisKey, 0)
//	return movieChan, errorChan
//}
//
//func(h *TileServiceHandler) AsyncFetch(ctx context.Context,
//	movieTileChan chan <- TileService.MovieTile,
//	errorChan chan <- error,
//	redisKey string,
//	startCursor uint64){
//
//	defer close(movieTileChan)
//	defer close(errorChan)
//
//	for{
//		select {
//		case <-ctx.Done(): // Context was done (timeout, cancel, etc.)
//			errorChan <- ctx.Err() // If canceled, ctx.Err() would return the context.Canceled error
//			return
//		default:
//		}
//		resultSet, serverCursor, err := h.RedisConnection.SScan(redisKey, startCursor, "", 20 ).Result()
//		if err != nil {
//			errorChan <- err
//			return
//		}
//		if serverCursor == 0 {
//			return
//		}
//		startCursor = serverCursor
//		for _,v := range resultSet {
//			var resultMovie TileService.MovieTile
//			err := proto.Unmarshal([]byte(v), &resultMovie)
//			if err != nil {
//				errorChan <- err
//			}
//			movieTileChan <- resultMovie
//		}
//	}
//}





//TODO redis Data Trial 1
// Getting user specific Language
//result := h.RedisConnection.SMembers(fmt.Sprintf("user:%s:languages", req.UserId)).Val()
//for i,k := range result{
//result[i] = "cloudwalker:languages:"+k
//}
//result = append(result, req.RowId)
//tempRedisKey := req.UserId+":temp"
////Get comman tiles for that rows with respect
//h.RedisConnection.SInterStore(tempRedisKey, result...)
//var nextCursor uint64
//for {
//resultSet, serverCursor, err := h.RedisConnection.SScan(tempRedisKey, nextCursor, "", 100).Result()
//if err != nil {
//return err
//}
//nextCursor = serverCursor
//resultTiles := h.RedisConnection.HMGet("cloudwalkerTiles", resultSet...)
//for _, v := range resultTiles.Val() {
//var resultMovie TileService.MovieTile
//err := proto.Unmarshal(GetBytes(v), &resultMovie)
//if err != nil {
//return err
//}
//err = stream.Send(&resultMovie)
//if err != nil {
//return err
//}
//}
//if serverCursor == 0 {
//err := stream.Close()
//if err != nil {
//return err
//}
//break
//}
//}
//h.RedisConnection.Del(tempRedisKey)
