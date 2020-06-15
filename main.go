package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	stream = flag.String("stream", "kinesis-poc", "kinesis-poc")
	region = flag.String("region", "us-west-2", "us-west-2")
)

var wg sync.WaitGroup
var streamName = aws.String("kinesis-poc")

func main() {
	wg.Add(5)
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	os.Setenv("AWS_ACCESS_KEY", "")

	os.Setenv("AWS_SECRET_ACCESS_KEY", "")
	os.Setenv("AWS_SECRET_KEY", "")
	flag.Parse()

	fmt.Printf("region %v\n", *region)
	fmt.Printf("stream %v\n", *stream)

	s := session.New(&aws.Config{Region: aws.String(*region)})
	kc := kinesis.New(s)

	/*
		_, err := kc.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(3),
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}
		//fmt.Printf("%v\n", out)

		if err := kc.WaitUntilStreamExists(&kinesis.DescribeStreamInput{StreamName: streamName}); err != nil {
			panic(err)
		}
	*/

	_, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%v\n", streams)

	// Declare a unbuffered channel
	shardChanPub := make(chan string)
	shardChanSub := make(chan string)
	go pub(shardChanPub, kc)
	go buildShards(shardChanSub, shardChanPub, kc)
	//go sub(shardChanSub, kc)
	wg.Wait()
	// OK, finally delete your stream
	deleteOutput, err := kc.DeleteStream(&kinesis.DeleteStreamInput{
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", deleteOutput)
}

func pub(shardChanOut chan<- string, kc *kinesis.Kinesis) {
	defer wg.Done()
	shardChanOut <- ("INIT")
	t := time.Now()
	dateFormat := t.Format("2006-01-02 15:04:05.000")
	/*

		putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
			Data:         []byte(dateFormat),
			StreamName:   streamName,
			PartitionKey: aws.String("key1"),
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("\n")
		t = time.Now()
		dateFormat = t.Format("2006-01-02 15:04:05.000")
		fmt.Printf("@time %s Data Pub --> key: %v --> on shard %v ", dateFormat, *aws.String("key1"), *putOutput.ShardId)
	*/

	for i := 0; i < 5; i++ {
		// put 2 records using PutRecords API
		t := time.Now()
		entries := make([]*kinesis.PutRecordsRequestEntry, 2)
		for k := 0; k < len(entries); k++ {
			entries[k] = &kinesis.PutRecordsRequestEntry{
				Data:         []byte(fmt.Sprintf("data-%s", t.Format("2006-01-02 15:04:05.000"))),
				PartitionKey: aws.String(fmt.Sprintf("kay%d", i)),
			}
		}

		sleep := rand.Int63n(1000)
		time.Sleep(time.Duration(sleep) * time.Millisecond)

		//fmt.Printf("%v\n", entries)
		putsOutput, err := kc.PutRecords(&kinesis.PutRecordsInput{
			Records:    entries,
			StreamName: streamName,
		})
		if err != nil {
			panic(err)
		}
		t = time.Now()
		dateFormat = t.Format("2006-01-02 15:04:05.000")
		fmt.Printf("@time %s Data Pub --> key: %v --> on shard %s \n", dateFormat, fmt.Sprintf("kay%d", i), *putsOutput.Records[0].ShardId)
	}

	// putsOutput has Records, and its shard id and sequece enumber.
	//fmt.Printf("putsOutput key2 %v\n", putsOutput.Records)
	//var shardIDVal = string("")
	//for i := 0; i < len(putsOutput.Records); i++ {
	//	if shardIDVal != aws.StringValue(putsOutput.Records[i].ShardId) {
	//		shardChanOut <- aws.StringValue(putsOutput.Records[i].ShardId)
	//	}
	//}

	//fmt.Printf("%v\n", putOutput.ShardId)
	/*
		fmt.Printf("\n")
		if shardIDVal != aws.StringValue(putOutput.ShardId) {
			//shardChanOut <- aws.StringValue(putOutput.ShardId)
		}
	*/
	close(shardChanOut)
}

func buildShards(shardChanOut chan<- string, shardChanIn <-chan string, kc *kinesis.Kinesis) {
	defer wg.Done()
	shardID := <-shardChanIn
	fmt.Printf("buildShards Received shardID %v\n", shardID)
	// Example sending a request using the ListShardsRequest method.

	listShardsOutput, err := kc.ListShards(&kinesis.ListShardsInput{
		StreamName: streamName,
	})
	//req, resp := kc.ListShardsRequest(nil)

	//err := req.Send()
	if err != nil { // resp is now filled
		fmt.Println(err)
	}

	//fmt.Printf("\n%v", listShardsOutput)
	//fmt.Printf("listShardsOutput\n")
	for i := 0; i < len(listShardsOutput.Shards); i++ {
		fmt.Printf("ShardId %v\n", *listShardsOutput.Shards[i].ShardId)

		//shardChanOut <- aws.StringValue(aws.String(*listShardsOutput.Shards[i].ShardId))
		go sub(*listShardsOutput.Shards[i].ShardId, kc, i)
	}
	fmt.Printf("\n")

	//fmt.Printf("resp ID %v\n", listShardsOutput)
	//shardChanOut <- aws.StringValue(aws.String(shardID))
	sleep := rand.Int63n(5000)
	time.Sleep(time.Duration(sleep) * time.Millisecond)
	close(shardChanOut)
}

// sub ...
func sub(shardID string, kc *kinesis.Kinesis, goRoutineNum int) {
	defer wg.Done()
	//streamName := aws.String("kinesis-poc")
	//shardID := shardChanIn
	fmt.Printf("Received shardID %v\n", shardID)
	sleep := rand.Int63n(5000)
	time.Sleep(time.Duration(sleep) * time.Millisecond)
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		// ShardIteratorType: aws.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: aws.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%v\n", iteratorOutput)
	ShardIterator := iteratorOutput.ShardIterator
	for true {
		// get records use shard iterator for making request
		records, err := kc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: ShardIterator,
		})
		if err != nil {
			panic(err)
		}
		//fmt.Printf(" records %v\n", records)
		fmt.Printf("\n")
		for i := 0; i < len(records.Records); i++ {
			t := time.Now()
			fmt.Printf("@time %s routineNum: %v PartitionKey %v data: %s shardID => %v", t.Format("2006-01-02 15:04:05.000"), goRoutineNum, *records.Records[i].PartitionKey, records.Records[i].Data, shardID)
			fmt.Printf("\n")
		}

		sleep = rand.Int63n(5000)
		time.Sleep(time.Duration(sleep) * time.Millisecond)
		ShardIterator = records.NextShardIterator
		t := time.Now()
		dateFormat := t.Format("2006-01-02 15:04:05.000")
		fmt.Printf("@time %s routineNum: %v NextShardIterator \n", dateFormat, goRoutineNum)
	}
}

// subss ...
func subss(shardChanIn <-chan string, kc *kinesis.Kinesis) {
	defer wg.Done()
	//streamName := aws.String("kinesis-poc")
	shardID := <-shardChanIn

	fmt.Printf("Received shardID %v\n", shardID)
	sleep := rand.Int63n(1000)
	time.Sleep(time.Duration(sleep) * time.Millisecond)
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String("LATEST"),
		// ShardIteratorType: aws.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: aws.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%v\n", iteratorOutput)

	// get records use shard iterator for making request
	records, err := kc.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iteratorOutput.ShardIterator,
	})
	if err != nil {
		panic(err)
	}
	//fmt.Printf(" records %v\n", records)
	fmt.Printf("\n")
	for i := 0; i < len(records.Records); i++ {
		fmt.Printf("PartitionKey %v data: %v shardID => %v \n", *records.Records[i].PartitionKey, records.Records[i].Data, shardID)
		//fmt.Printf("Data %s\n", records.Records[i].Data)
		fmt.Printf("\n")
	}
}
