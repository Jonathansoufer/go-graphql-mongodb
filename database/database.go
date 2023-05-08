package database

import (
	"context"
	"log"
	"time"

	"github.com/Jonathansoufer/go-graphql-mongodb/graph/model"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)
var connectString string = "mongodb://localhost:27017"

type DB struct {
	client *mongo.Client
}

func Connect() *DB {
	client, err := mongo.NewClient(options.Client().ApplyURI(connectString))

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel:= context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	return &DB{client: client}
}

func (db *DB) GetJobs() []*model.JobListing {
	var jobs []*model.JobListing
	collection := db.client.Database("jobs").Collection("listings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cursor, err := collection.Find(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var job model.JobListing
		cursor.Decode(&job)
		jobs = append(jobs, &job)
	}
	return jobs
}

func (db *DB) GetJob(id string) *model.JobListing {
	var job model.JobListing
	collection := db.client.Database("jobs").Collection("listings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := collection.FindOne(ctx, model.JobListing{ID: id}).Decode(&job)
	if err != nil {
		log.Fatal(err)
	}
	return &job
}

func (db *DB) CreateJobListing(input model.CreateJobListingInput) *model.JobListing {
	collection := db.client.Database("jobs").Collection("listings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := collection.InsertOne(ctx, model.JobListing{Title: input.Title, Description: input.Description, Company: input.Company, URL: input.URL})
	if err != nil {
		log.Fatal(err)
	}
	return &model.JobListing{ID: result.InsertedID.(string), Title: input.Title, Description: input.Description, Company: input.Company, URL: input.URL}
}

func (db *DB) UpdateJobListing(id string, input model.UpdateJobListingInput) *model.JobListing {
	collection := db.client.Database("jobs").Collection("listings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var update = make(map[string]interface{})
	if input.Title != nil {
		update["title"] = *input.Title
	}
	if input.Description != nil {
		update["description"] = *input.Description
	}
	if input.Company != nil {
		update["company"] = *input.Company
	}
	if input.URL != nil {
		update["url"] = *input.URL
	}
	_, err := collection.UpdateOne(ctx, model.JobListing{ID: id}, update)
	if err != nil {
		log.Fatal(err)
	}
	return db.GetJob(id)
}

func (db *DB) DeleteJobListing(id string) *model.DeleteJobResponse {
	collection := db.client.Database("jobs").Collection("listings")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := collection.DeleteOne(ctx, model.JobListing{ID: id})
	if err != nil {
		log.Fatal(err)
	}
	return &model.DeleteJobResponse{DeletedJobID: id}
}