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
	fmt.Println(restProvider.All(ctx))
	sqlConfig := data.ProviderConfig{
		Type:      "sqlite",
		TableName: "items",
		IDColumn:  "id",
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
	/*// Create the SQLite table if it does not exist.
	// We assume that the underlying SQL connection can be accessed from the SQLProvider.
	if sp, ok := sqlProvider.(*data.SQLProvider); ok {
		// Construct a CREATE TABLE statement using the table and id column from configuration.
		createTableStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			%s TEXT PRIMARY KEY,
			name TEXT,
			time TEXT
		);`, sp.Config.TableName, sp.Config.IDColumn)

		// Execute the statement on the underlying database connection.
		// Here we assume that sp.DB is a *sql.DB.
		if _, err := sp.Exec(ctx, createTableStmt); err != nil {
			log.Fatalf("Failed to create SQLite table: %v", err)
		} else {
			log.Printf("SQLite table %s ensured.", sp.Config.TableName)
		}
	} else {
		log.Printf("SQLProvider is not of expected type; skipping table creation.")
	}*/
	testDataProvider(ctx, "SQLProvider", sqlProvider)
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

}
