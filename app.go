package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"

	"cloud.google.com/go/bigquery"
)

type Data struct {
	Imei                string  `json:"Imei"`
	Latitude            float64 `json:"Latitude,string"`
	Longitude           float64 `json:"Longitude,string"`
	GPSDate             string  `json:"GPSDate"`
	Heading             string  `json:"Heading"`
	EventID             string  `json:"EventID"`
	EventName           string  `json:"EventName"`
	AdditionalEventInfo string  `json:"AdditionalEventInfo"`
	Odometer            float64 `json:"Odometer,string"`
	EngineHours         string  `json:"EngineHours"`
	VehicleSpeed        float64 `json:"VehicleSpeed,string"`
	RoadSpeedLimit      string  `json:"RoadSpeedLimit"`
	RoadName            string  `json:"RoadName"`
	DriverID            string  `json:"DriverID"`
	IsIgnitionOn        bool    `json:"IsIgnitionOn"`
	Altitude            float64 `json:"Altitude,string"`
	CanBusData          string  `json:"CanBusData"`
	Attributes          string  `json:"Attributes"`
	RawData             string  `json:"RawData"`
	Protocol            string  `json:"Protocol"`
}

// insertRows demonstrates inserting data into a table using the streaming insert mechanism.
func insertRows(projectID, datasetID, tableID string, addr net.Addr, data []byte) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer client.Close()

	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	p_data, err := processPacket(data)
	if err != nil {
		log.Println("2: ", err)
	}
	items := []*Data{&p_data}

	if err := inserter.Put(ctx, items); err != nil {
		log.Println("4: ", err)
		return err
	}
	return nil
}

func processPacket(buffer []byte) (Data, error) {
	// Unmarshal the JSON data into a data struct
	var data Data
	err := json.Unmarshal(buffer, &data)
	if err != nil {
		log.Println("3: ", err)
		return data, err
	}

	fmt.Printf("Processing packet: %s\n", buffer)

	return data, nil
}

func main() {
	projectID := os.Getenv("PROJECT_ID")
	if projectID == "" {
		fmt.Println("PROJECT_ID environment variable must be set.")
		os.Exit(1)
	}

	datasetID := os.Getenv("DATASET_ID")
	if datasetID == "" {
		fmt.Println("DATASET_ID environment variable must be set.")
		os.Exit(1)
	}

	tableID := os.Getenv("TABLE_ID")
	if tableID == "" {
		fmt.Println("TABLE_ID environment variable must be set.")
		os.Exit(1)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	// Listen for incoming UDP packets on port 8000
	pc, err := net.ListenPacket("udp", ":"+port)
	if err != nil {
		log.Fatal("0: ", err)
	}
	defer pc.Close()

	fmt.Println("UDP server listening on port " + port + "...")

	// Continuously receive and process UDP packets
	for {
		buffer := make([]byte, 5120)
		n, addr, err := pc.ReadFrom(buffer)
		if err != nil {
			log.Println("1: ", err)
			continue
		}
		insertRows(projectID, datasetID, tableID, addr, buffer[:n])
	}

}
