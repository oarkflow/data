package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/data"
	"github.com/oarkflow/data/utils"
	"github.com/oarkflow/squealx"
)

func populateProvider(ctx context.Context, provider data.Provider, idField string, count int) error {
	for i := 0; i < count; i++ {
		item := utils.Record{
			idField: fmt.Sprintf("stream_item_%d", i),
			"name":  fmt.Sprintf("Streaming Item %d", i),
			"time":  time.Now().Format(time.RFC3339),
		}
		if err := provider.Create(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

type RESTServerConfig struct {
	ResourcePath string
	IDField      string
}

var (
	restStore = make(map[string]utils.Record)
	restMu    sync.RWMutex
)

func startRESTServerWithConfig(config RESTServerConfig) {
	resourcePath := "/" + strings.Trim(config.ResourcePath, "/")
	idField := config.IDField

	http.HandleFunc(resourcePath, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			var item utils.Record
			if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			id, ok := item[idField].(string)
			if !ok {
				http.Error(w, fmt.Sprintf("missing id field %s", idField), http.StatusBadRequest)
				return
			}
			restMu.Lock()
			restStore[id] = item
			restMu.Unlock()
			w.WriteHeader(http.StatusCreated)
		case "GET":
			restMu.RLock()
			items := make([]utils.Record, 0, len(restStore))
			for _, item := range restStore {
				items = append(items, item)
			}
			restMu.RUnlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc(resourcePath+"/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 3 {
			http.Error(w, "invalid URL", http.StatusBadRequest)
			return
		}
		id := parts[2]
		switch r.Method {
		case "GET":
			restMu.RLock()
			item, ok := restStore[id]
			restMu.RUnlock()
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(item)
		case "PUT":
			var item utils.Record
			if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			restMu.Lock()
			if _, ok := restStore[id]; !ok {
				restMu.Unlock()
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			restStore[id] = item
			restMu.Unlock()
			w.WriteHeader(http.StatusOK)
		case "DELETE":
			restMu.Lock()
			if _, ok := restStore[id]; !ok {
				restMu.Unlock()
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			delete(restStore, id)
			restMu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	log.Printf("Starting REST server on :8081 with resourcePath %s and idField %s", resourcePath, idField)
	go func() {
		_ = http.ListenAndServe(":8081", nil)
	}()
}

func testDataProvider(ctx context.Context, name string, provider data.Provider) {
	log.Printf("Testing %s", name)
	var idField string
	switch p := provider.(type) {
	case *data.SQLProvider:
		idField = p.Config.IDColumn
	case *data.RESTProvider:
		idField = p.IdField
	case *data.JSONFileProvider:
		idField = p.Config.IDField
	case *data.CSVFileProvider:
		idField = p.Config.IDColumn
	case *data.RedisProvider:
		idField = p.Config.IDField
	default:
		idField = "id"
	}

	item := utils.Record{
		idField: fmt.Sprintf("%s_item", name),
		"name":  "Test Item",
		"time":  time.Now().Format(time.RFC3339),
	}

	if err := provider.Create(ctx, item); err != nil {
		log.Printf("[%s] Create error: %v", name, err)
		return
	}

	readItem, err := provider.Read(ctx, item[idField].(string))
	if err != nil {
		log.Printf("[%s] Read error: %v", name, err)
		return
	}
	log.Printf("[%s] Read item: %v", name, readItem)

	allItems, err := provider.All(ctx)
	if err != nil {
		log.Printf("[%s] All() error: %v", name, err)
	} else {
		log.Printf("[%s] All() returned %d items", name, len(allItems))
	}

	item["name"] = "Updated Item"
	if err := provider.Update(ctx, item); err != nil {
		log.Printf("[%s] Update error: %v", name, err)
		return
	}
	updatedItem, err := provider.Read(ctx, item[idField].(string))
	if err != nil {
		log.Printf("[%s] Read after update error: %v", name, err)
		return
	}
	log.Printf("[%s] Updated item: %v", name, updatedItem)

	if err := provider.Delete(ctx, item[idField].(string)); err != nil {
		log.Printf("[%s] Delete error: %v", name, err)
		return
	}
	_, err = provider.Read(ctx, item[idField].(string))
	if err != nil {
		log.Printf("[%s] Successfully deleted item", name)
	} else {
		log.Printf("[%s] Delete did not remove item", name)
	}
}

func main() {
	ctx := context.Background()

	restServerConfig := RESTServerConfig{
		ResourcePath: "items",
		IDField:      "id",
	}
	go startRESTServerWithConfig(restServerConfig)
	time.Sleep(1 * time.Second)

	sqlConfig := data.ProviderConfig{
		Type:       "sqlite",
		TableName:  "items",
		IDColumn:   "id",
		DataColumn: "data",
	}
	sqlConfig.Database = "data.db"
	sqlConfig.Driver = "sqlite"
	sqlProvider, err := data.NewProvider(sqlConfig)
	if err != nil {
		log.Fatalf("SQLProvider error: %v", err)
	}
	defer func() {
		_ = sqlProvider.Close()
	}()
	testDataProvider(ctx, "SQLProvider", sqlProvider)

	restConfig := data.ProviderConfig{
		Type:         "rest",
		BaseURL:      "http://localhost:8081",
		Timeout:      5 * time.Second,
		ResourcePath: "items",
		IDColumn:     "id",
	}
	restProvider, err := data.NewProvider(restConfig)
	if err != nil {
		log.Fatalf("RESTProvider error: %v", err)
	}
	testDataProvider(ctx, "RESTProvider", restProvider)

	jsonConfig := data.ProviderConfig{
		Type:     "json",
		FilePath: "data.json",
		IDColumn: "id",
	}
	jsonFileProvider, err := data.NewProvider(jsonConfig)
	if err != nil {
		log.Fatalf("JSONFileProvider error: %v", err)
	}
	if err := jsonFileProvider.Setup(ctx); err != nil {
		log.Fatalf("JSONFileProvider Setup error: %v", err)
	}
	testDataProvider(ctx, "JSONFileProvider", jsonFileProvider)

	csvConfig := data.ProviderConfig{
		Type:       "csv",
		FilePath:   "data.csv",
		IDColumn:   "id",
		DataColumn: "data",
	}
	csvFileProvider, err := data.NewProvider(csvConfig)
	if err != nil {
		log.Fatalf("JSONFileProvider error: %v", err)
	}
	if err := csvFileProvider.Setup(ctx); err != nil {
		log.Fatalf("CSVFileProvider Setup error: %v", err)
	}
	testDataProvider(ctx, "CSVFileProvider", csvFileProvider)

	redisConfig := data.ProviderConfig{
		Type: "redis",
		Config: squealx.Config{
			Host:     "localhost:6379",
			Password: "",
			Database: "0",
		},
		IDColumn: "id",
	}
	redisProvider, err := data.NewProvider(redisConfig)
	if err != nil {
		log.Fatalf("RedisProvider error: %v", err)
	}
	if err := redisProvider.Setup(ctx); err != nil {
		log.Printf("RedisProvider Setup error: %v", err)
	} else {
		testDataProvider(ctx, "RedisProvider", redisProvider)
	}
	_ = redisProvider.Close()

	log.Println("Populating SQLProvider with data for streaming test...")
	if err := populateProvider(ctx, sqlProvider, sqlConfig.IDColumn, 5); err != nil {
		log.Printf("Error populating SQLProvider for streaming: %v", err)
	}

	log.Println("Streaming data from SQLProvider:")
	if streamer, ok := sqlProvider.(data.StreamingProvider); ok {
		streamCh, errCh := streamer.Stream(ctx)
		for {
			select {
			case item, ok := <-streamCh:
				if !ok {
					streamCh = nil
				} else {
					log.Printf("Streamed item: %v", item)
				}
			case err, ok := <-errCh:
				if ok && err != nil {
					log.Printf("Stream error: %v", err)
				}
				errCh = nil
			}
			if streamCh == nil && errCh == nil {
				break
			}
		}
	}
}
