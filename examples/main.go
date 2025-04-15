package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/data"
)

func testDataProvider(ctx context.Context, name string, provider data.Provider) {
	log.Printf("Testing %s", name)
	var idField string
	switch p := provider.(type) {
	case *data.SQLProvider:
		idField = p.Config.IDColumn
	case *data.JSONFileProvider:
		idField = p.Config.IDField
	case *data.CSVFileProvider:
		idField = p.Config.IDColumn
	case *data.RedisProvider:
		idField = p.Config.IDField
	default:
		idField = "id"
	}

	item := data.Record{
		idField: fmt.Sprintf("%s_item", name),
		"name":  "Test Item",
		"time":  time.Now().Format(time.DateTime),
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

	if err := provider.Update(ctx, item); err != nil {
		log.Printf("[%s] Update error: %v", name, err)
		return
	}
	fmt.Println(item)
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

	restConfig := data.ProviderConfig{
		Type:    "rest",
		Timeout: 5 * time.Second,
		Endpoints: data.EndpointsConfig{
			Create: data.EndpointConfig{
				URL:     "https://jsonplaceholder.typicode.com/posts",
				Method:  "POST",
				IDField: "id",
			},
			Read: data.EndpointConfig{
				URL:     "https://jsonplaceholder.typicode.com/posts/%v",
				Method:  "GET",
				IDField: "id",
			},
			Update: data.EndpointConfig{
				URL:     "https://jsonplaceholder.typicode.com/posts/%v",
				Method:  "PUT",
				IDField: "id",
			},
			Delete: data.EndpointConfig{
				URL:     "https://jsonplaceholder.typicode.com/posts/%v",
				Method:  "DELETE",
				IDField: "id",
			},
			All: data.EndpointConfig{
				URL:    "https://jsonplaceholder.typicode.com/posts",
				Method: "GET",
			},
		},
		IDColumn: "id",
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
		Type:        "csv",
		FilePath:    "data.csv",
		IDColumn:    "id",
		DataColumns: []string{"name", "time"},
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
		Type:       "redis",
		IDColumn:   "id", // the record field that will act as a unique identifier
		KeyPattern: "*",
	}
	redisConfig.Host = "localhost:6379"
	redisConfig.Password = ""
	redisConfig.Database = "0"

	redisProvider, err := data.NewProvider(redisConfig)
	if err != nil {
		log.Fatalf("RedisProvider error: %v", err)
	}
	// Ensure connection is alive.
	if err := redisProvider.Setup(ctx); err != nil {
		log.Fatalf("RedisProvider Setup error: %v", err)
	}
	defer func() {
		_ = redisProvider.Close()
	}()
	testDataProvider(ctx, "RedisProvider", redisProvider)

	// --- MySQL Provider Example ---
	// Configure the MySQL connection details. Adjust the Database, Host, Port,
	// Username, and Password as needed for your environment.
	mysqlConfig := data.ProviderConfig{
		Type:        "mysql",
		TableName:   "items", // table to use for storing records
		IDColumn:    "items_id",
		DataColumns: []string{"id", "name", "time"},
		Timeout:     5 * time.Second,
		Queries:     data.Queries{
			// Optionally, provide custom queries (or leave empty to use default table operations)
			// For example:
			// QueryCreate: "INSERT INTO items (id, name, time) VALUES (?, ?, ?)",
			// QueryRead:   "SELECT id, name, time FROM items WHERE id = ?",
			// QueryUpdate: "UPDATE items SET name = ?, time = ? WHERE id = ?",
			// QueryDelete: "DELETE FROM items WHERE id = ?",
			// QueryAll:    "SELECT id, name, time FROM items",
		},
	}
	// Additional MySQL connection settings can be provided via the embedded squealx.Config.
	// For example:
	mysqlConfig.Host = "localhost"
	mysqlConfig.Driver = "mysql"
	mysqlConfig.Port = 3306
	mysqlConfig.Username = "root"
	mysqlConfig.Password = "root"
	mysqlConfig.Database = "sujit"

	mysqlProvider, err := data.NewProvider(mysqlConfig)
	if err != nil {
		log.Fatalf("MySQLProvider error: %v", err)
	}
	defer func() {
		_ = mysqlProvider.Close()
	}()
	// Ensure MySQL connection is alive.
	if err := mysqlProvider.Setup(ctx); err != nil {
		log.Fatalf("MySQLProvider Setup error: %v", err)
	}
	testDataProvider(ctx, "MySQLProvider", mysqlProvider)

}
