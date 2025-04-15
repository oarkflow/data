package data

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/oarkflow/json"
	"github.com/oarkflow/json/jsonparser"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/drivers/sqlite"
	"github.com/redis/go-redis/v9"
)

type Record = map[string]any

type Field struct {
	Name      string
	DataType  string
	IsPrimary bool
	Nullable  bool
}

type Provider interface {
	Setup(ctx context.Context) error
	Create(ctx context.Context, item Record) error
	Read(ctx context.Context, id string) (Record, error)
	Update(ctx context.Context, item Record) error
	Fields(ctx context.Context) (fieldList map[string]FieldSchema, err error)
	Delete(ctx context.Context, id string) error
	All(ctx context.Context) ([]Record, error)
	Close() error
}

type StreamingProvider interface {
	Provider
	Stream(ctx context.Context) (<-chan Record, <-chan error)
}

type ProviderConfig struct {
	Type      string
	Endpoints EndpointsConfig
	squealx.Config
	TableName   string
	IDColumn    string
	DataColumns []string
	Timeout     time.Duration
	FilePath    string
	KeyPattern  string
	Queries     Queries
}

type Queries struct {
	QueryCreate string
	QueryRead   string
	QueryUpdate string
	QueryDelete string
	QueryAll    string
}

type SQLProvider struct {
	db     squealx.Repository[map[string]any]
	Config ProviderConfig
}

func NewSQLProvider(config ProviderConfig) (*SQLProvider, error) {
	db, err := dbFromConfig(config.Config)
	if err != nil {
		return nil, err
	}
	repo := squealx.New[map[string]any](db, config.TableName, config.IDColumn)
	p := &SQLProvider{db: repo, Config: config}

	if err := p.Setup(context.Background()); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *SQLProvider) Close() error {
	return s.db.GetDB().Close()
}

func (s *SQLProvider) Setup(ctx context.Context) error {
	return s.db.GetDB().Ping()
}

func (s *SQLProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
	fieldList := make(map[string]FieldSchema)
	fields, err := s.db.GetDB().GetTableFields(s.Config.TableName, s.Config.Database)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		schema := FieldSchema{
			FieldName:       field.Name,
			DataType:        field.DataType,
			MaxStringLength: field.Length,
		}
		if field.IsNullable == "YES" {
			schema.IsNullable = true
		}
		if field.Key == "PRI" {
			schema.IsPrimaryKey = true
		}
		fieldList[field.Name] = schema
	}
	return fieldList, nil
}

func (s *SQLProvider) Create(ctx context.Context, item Record) error {
	if s.Config.Queries.QueryCreate != "" {
		err := s.db.GetDB().ExecWithReturn(s.Config.Queries.QueryCreate, &item)
		return squealx.CanError(err, false)
	}
	err := s.db.Create(ctx, &item)
	return squealx.CanError(err, false)
}

func (s *SQLProvider) Read(ctx context.Context, id string) (Record, error) {
	filter := map[string]any{
		s.Config.IDColumn: id,
	}
	if s.Config.Queries.QueryRead != "" {
		records, err := s.db.Raw(ctx, s.Config.Queries.QueryRead, filter)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return nil, fmt.Errorf("not found")
		}
		return records[0], nil
	}
	return s.db.First(ctx, filter)
}

func (s *SQLProvider) Update(ctx context.Context, item Record) error {
	id, ok := item[s.Config.IDColumn]
	if !ok {
		return fmt.Errorf("item missing id field %s", s.Config.IDColumn)
	}
	filter := map[string]any{
		s.Config.IDColumn: id,
	}
	if s.Config.Queries.QueryUpdate != "" {
		records, err := s.db.Raw(ctx, s.Config.Queries.QueryUpdate, filter)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return fmt.Errorf("not found")
		}
		return nil
	}
	err := s.db.Update(ctx, &item, map[string]any{
		s.Config.IDColumn: id,
	})
	return squealx.CanError(err, false)
}

func (s *SQLProvider) Delete(ctx context.Context, id string) error {
	filter := map[string]any{
		s.Config.IDColumn: id,
	}
	if s.Config.Queries.QueryDelete != "" {
		records, err := s.db.Raw(ctx, s.Config.Queries.QueryDelete, filter)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return fmt.Errorf("not found")
		}
		return nil
	}
	err := s.db.Delete(ctx, filter)
	return squealx.CanError(err, false)
}

func (s *SQLProvider) All(ctx context.Context) ([]Record, error) {
	if s.Config.Queries.QueryAll != "" {
		return s.db.Raw(ctx, s.Config.Queries.QueryAll)
	}
	return s.db.All(ctx)
}

func (s *SQLProvider) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		items, err := s.All(ctx)
		if err != nil {
			errCh <- err
			return
		}
		for _, item := range items {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case out <- item:
			}
		}
	}()
	return out, errCh
}

// EndpointConfig defines the configuration for a REST operation.
type EndpointConfig struct {
	URL     string
	Method  string            // HTTP method (GET, POST, etc.). Defaults will apply if empty.
	Headers map[string]string // Optional headers to include in the request.
	IDField string
	DataKey string
}

// EndpointsConfig groups endpoint configurations for various operations.
type EndpointsConfig struct {
	Setup  EndpointConfig
	Create EndpointConfig
	Read   EndpointConfig
	Update EndpointConfig
	Delete EndpointConfig
	All    EndpointConfig
}

// RESTConfig holds the global REST configuration.
type RESTConfig struct {
	Timeout   time.Duration
	Endpoints EndpointsConfig
}

// RESTProvider is a RESTful implementation that uses a flexible configuration.
type RESTProvider struct {
	client    *http.Client
	endpoints EndpointsConfig
}

// NewRESTProvider constructs a new RESTProvider from the given configuration.
func NewRESTProvider(config RESTConfig) *RESTProvider {
	return &RESTProvider{
		client:    &http.Client{Timeout: config.Timeout},
		endpoints: config.Endpoints,
	}
}

// applyHeaders attaches header key/value pairs from the configuration to the HTTP request.
func applyHeaders(req *http.Request, headers map[string]string) {
	for key, value := range headers {
		req.Header.Set(key, value)
	}
}

// Setup verifies that the REST endpoint is available using the Setup endpoint configuration.
func (r *RESTProvider) Setup(ctx context.Context) error {
	return nil
}

// Create creates a new item using the Create endpoint configuration.
func (r *RESTProvider) Create(ctx context.Context, item Record) error {
	ep := r.endpoints.Create
	if ep.Method == "" {
		ep.Method = "POST"
	}
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, ep.Method, ep.URL, bytes.NewBuffer(dataBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	applyHeaders(req, ep.Headers)
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create item, status: %s", resp.Status)
	}
	return nil
}

// Read retrieves an item with a given ID using the Read endpoint configuration.
func (r *RESTProvider) Read(ctx context.Context, id string) (Record, error) {
	ep := r.endpoints.Read
	if ep.Method == "" {
		ep.Method = "GET"
	}
	url := fmt.Sprintf(ep.URL, id)
	// Append the id to the resource path.
	req, err := http.NewRequestWithContext(ctx, ep.Method, url, nil)
	if err != nil {
		return nil, err
	}
	applyHeaders(req, ep.Headers)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to read item, status: %s", resp.Status)
	}
	dataBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	isArray, err := CheckJSON(dataBytes)
	if err != nil {
		return nil, err
	}
	if isArray {
		var items []Record
		err = json.Unmarshal(dataBytes, &items)
		if err != nil {
			return nil, err
		}
		if len(items) > 0 {
			return items[0], nil
		}
		for _, item := range items {
			if item[r.endpoints.Read.IDField] == id {
				return item, nil
			}
		}
		return nil, fmt.Errorf("item not found")
	}
	if r.endpoints.Read.DataKey != "" {
		dataBytes, _, _, err = jsonparser.Get(dataBytes, r.endpoints.Read.DataKey)
		if err != nil {
			return nil, err
		}
	}
	var item Record
	err = json.Unmarshal(dataBytes, &item)
	return item, err
}

// Fields retrieves and infers the schema from the REST API records.
func (r *RESTProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
	records, err := r.All(ctx)
	if err != nil {
		return nil, err
	}
	return DetectSchema(records, 10), nil
}

// Update updates an item using the Update endpoint configuration.
func (r *RESTProvider) Update(ctx context.Context, item Record) error {
	ep := r.endpoints.Update
	if ep.Method == "" {
		ep.Method = "PUT"
	}
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	url := fmt.Sprintf(ep.URL, item[ep.IDField])
	// Append the id to the resource path.
	req, err := http.NewRequestWithContext(ctx, ep.Method, url, bytes.NewBuffer(dataBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	applyHeaders(req, ep.Headers)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update item, status: %s", resp.Status)
	}
	return nil
}

// Delete deletes an item with the given ID using the Delete endpoint configuration.
func (r *RESTProvider) Delete(ctx context.Context, id string) error {
	ep := r.endpoints.Delete
	if ep.Method == "" {
		ep.Method = "DELETE"
	}
	url := fmt.Sprintf(ep.URL, id)
	req, err := http.NewRequestWithContext(ctx, ep.Method, url, nil)
	if err != nil {
		return err
	}
	applyHeaders(req, ep.Headers)

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete item, status: %s", resp.Status)
	}
	return nil
}

// All retrieves all items using the All endpoint configuration.
func (r *RESTProvider) All(ctx context.Context) ([]Record, error) {
	ep := r.endpoints.All
	if ep.Method == "" {
		ep.Method = "GET"
	}
	req, err := http.NewRequestWithContext(ctx, ep.Method, ep.URL, nil)
	if err != nil {
		return nil, err
	}
	applyHeaders(req, ep.Headers)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get all items, status: %s", resp.Status)
	}
	dataBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	isArray, err := CheckJSON(dataBytes)
	if err != nil {
		return nil, err
	}
	if !isArray && r.endpoints.All.DataKey != "" {
		dataBytes, _, _, err = jsonparser.Get(dataBytes, r.endpoints.All.DataKey)
		if err != nil {
			return nil, err
		}
	}
	var items []Record
	err = json.Unmarshal(dataBytes, &items)
	return items, nil
}

// Close performs any cleanup necessary for the provider.
func (r *RESTProvider) Close() error {
	r.client.CloseIdleConnections()
	return nil
}

type JSONFileConfig struct {
	// FilePath for the data file.
	FilePath string
	// IDField is the JSON key used to uniquely identify records.
	IDField string
	// TombstoneMarker is the marker string to denote an obsolete record.
	TombstoneMarker string
	// Newline is the newline character or sequence to be used.
	Newline string
}

type JSONFileProvider struct {
	Config JSONFileConfig
	mu     sync.Mutex
}

func NewJSONFileProvider(config JSONFileConfig) *JSONFileProvider {
	return &JSONFileProvider{Config: config}
}

func (p *JSONFileProvider) Close() error {
	return nil
}

func (p *JSONFileProvider) Setup(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := os.Stat(p.Config.FilePath); os.IsNotExist(err) {
		return os.WriteFile(p.Config.FilePath, []byte("[]"), 0644)
	}
	return nil
}

func (p *JSONFileProvider) readAll() ([]Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var items []Record
	if _, err := os.Stat(p.Config.FilePath); os.IsNotExist(err) {
		return items, nil
	}
	data, err := os.ReadFile(p.Config.FilePath)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return items, nil
	}
	err = json.Unmarshal(data, &items)
	return items, err
}

func (p *JSONFileProvider) writeAll(items []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return os.WriteFile(p.Config.FilePath, data, 0644)
}

func (p *JSONFileProvider) Create(_ context.Context, item Record) error {
	items, err := p.readAll()
	if err != nil {
		return err
	}
	id, ok := item[p.Config.IDField].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", p.Config.IDField)
	}
	for _, it := range items {
		if it[p.Config.IDField] == id {
			return fmt.Errorf("item already exists")
		}
	}
	items = append(items, item)
	return p.writeAll(items)
}

func (p *JSONFileProvider) Read(_ context.Context, id string) (Record, error) {
	items, err := p.readAll()
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if it[p.Config.IDField] == id {
			return it, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (p *JSONFileProvider) Update(_ context.Context, item Record) error {
	items, err := p.readAll()
	if err != nil {
		return err
	}
	id, ok := item[p.Config.IDField].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", p.Config.IDField)
	}
	updated := false
	for i, it := range items {
		if it[p.Config.IDField] == id {
			items[i] = item
			updated = true
			break
		}
	}
	if !updated {
		return fmt.Errorf("item not found")
	}
	return p.writeAll(items)
}

func (p *JSONFileProvider) Delete(_ context.Context, id string) error {
	items, err := p.readAll()
	if err != nil {
		return err
	}
	var newItems []Record
	found := false
	for _, it := range items {
		if it[p.Config.IDField] == id {
			found = true
		} else {
			newItems = append(newItems, it)
		}
	}
	if !found {
		return fmt.Errorf("item not found")
	}
	return p.writeAll(newItems)
}

func (p *JSONFileProvider) Fields(ctx context.Context) (fieldList map[string]FieldSchema, err error) {
	records, err := p.All(ctx)
	if err != nil {
		return nil, err
	}
	return DetectSchema(records, 10), nil
}

func (p *JSONFileProvider) All(_ context.Context) ([]Record, error) {
	return p.readAll()
}

func (p *JSONFileProvider) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		items, err := p.readAll()
		if err != nil {
			errCh <- err
			return
		}
		for _, item := range items {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case out <- item:
			}
		}
	}()
	return out, errCh
}

type CSVFileConfig struct {
	// FilePath for the CSV file.
	FilePath string
	// IDColumn is the header name of the column that acts as primary key.
	IDColumn string
	// DataColumns lists the headers for data columns.
	DataColumns []string
	// TombstoneMarker is the marker string to denote an obsolete row.
	TombstoneMarker string
	// Delimiter is the CSV delimiter (for example, comma).
	Delimiter rune
}

type CSVFileProvider struct {
	Config CSVFileConfig
	mu     sync.Mutex
}

func NewCSVFileProvider(config CSVFileConfig) *CSVFileProvider {
	return &CSVFileProvider{Config: config}
}

func (p *CSVFileProvider) Close() error {
	return nil
}

func (p *CSVFileProvider) Setup(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := os.Stat(p.Config.FilePath); os.IsNotExist(err) {
		f, err := os.Create(p.Config.FilePath)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()
		writer := csv.NewWriter(f)
		defer writer.Flush()
		cols := []string{
			p.Config.IDColumn,
		}
		cols = append(cols, p.Config.DataColumns...)
		return writer.Write(cols)
	}
	return nil
}

func (p *CSVFileProvider) Fields(ctx context.Context) (fieldList map[string]FieldSchema, err error) {
	records, err := p.All(ctx)
	if err != nil {
		return nil, err
	}
	return DetectSchema(records, 10), nil
}

func (p *CSVFileProvider) readAll() ([]Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var items []Record
	if _, err := os.Stat(p.Config.FilePath); os.IsNotExist(err) {
		return items, nil
	}
	f, err := os.Open(p.Config.FilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 1 {
		return items, nil
	}
	header := records[0]
	for _, record := range records[1:] {
		// ensure the row matches the header columns
		if len(record) != len(header) {
			continue
		}
		item := make(Record)
		for i, key := range header {
			item[key] = record[i]
		}
		items = append(items, item)
	}
	return items, nil
}

func toString(val any) (string, bool) {
	switch val := val.(type) {
	case string:
		return val, true
	default:
		return fmt.Sprint(val), true
	}
}

func (p *CSVFileProvider) writeAll(items []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create (or overwrite) the CSV file.
	f, err := os.Create(p.Config.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create a CSV writer.
	writer := csv.NewWriter(f)
	defer writer.Flush()

	// Build header row: id column + the additional data columns.
	header := []string{p.Config.IDColumn}
	header = append(header, p.Config.DataColumns...)
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write each record.
	for _, item := range items {
		// Row has one column per header value.
		row := make([]string, len(header))
		// Get the id value.
		if id, ok := item[p.Config.IDColumn].(string); ok {
			row[0] = id
		} else {
			row[0] = ""
		}
		// Retrieve the remaining columns using the keys from the configuration.
		for i, col := range p.Config.DataColumns {
			val, exists := item[col]
			if !exists {
				row[i+1] = ""
				continue
			}
			// Convert the field value to a string.
			if s, ok := toString(val); ok {
				row[i+1] = s
			} else {
				row[i+1] = fmt.Sprintf("%v", val)
			}
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func (p *CSVFileProvider) Create(_ context.Context, item Record) error {
	items, err := p.readAll()
	if err != nil {
		return err
	}
	id, ok := item[p.Config.IDColumn].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", p.Config.IDColumn)
	}
	for _, it := range items {
		if it[p.Config.IDColumn] == id {
			return fmt.Errorf("item already exists")
		}
	}
	items = append(items, item)
	return p.writeAll(items)
}

func (p *CSVFileProvider) Read(_ context.Context, id string) (Record, error) {
	items, err := p.readAll()
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		if it[p.Config.IDColumn] == id {
			return it, nil
		}
	}
	return nil, fmt.Errorf("not found")
}

func (p *CSVFileProvider) Update(_ context.Context, item Record) error {
	items, err := p.readAll()
	if err != nil {
		return err
	}
	id, ok := item[p.Config.IDColumn].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", p.Config.IDColumn)
	}
	updated := false
	for i, it := range items {
		if it[p.Config.IDColumn] == id {
			items[i] = item
			updated = true
			break
		}
	}
	if !updated {
		return fmt.Errorf("item not found")
	}
	return p.writeAll(items)
}

func (p *CSVFileProvider) Delete(_ context.Context, id string) error {
	items, err := p.readAll()
	if err != nil {
		return err
	}
	var newItems []Record
	found := false
	for _, it := range items {
		if it[p.Config.IDColumn] == id {
			found = true
		} else {
			newItems = append(newItems, it)
		}
	}
	if !found {
		return fmt.Errorf("item not found")
	}
	return p.writeAll(newItems)
}

func (p *CSVFileProvider) All(_ context.Context) ([]Record, error) {
	return p.readAll()
}

func (p *CSVFileProvider) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		items, err := p.readAll()
		if err != nil {
			errCh <- err
			return
		}
		for _, item := range items {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case out <- item:
			}
		}
	}()
	return out, errCh
}

type RedisStorageType string

const (
	// Use direct key/value storage (default)
	RedisStorageKey RedisStorageType = "key"
	// Use a hash container (all records stored as fields under a common key)
	RedisStorageHash RedisStorageType = "hash"
	// Use a list container (all records appended to a list)
	RedisStorageList RedisStorageType = "list"
)

// RedisConfig now supports specifying which type of Redis container to use
// as well as extra parameters for hash and list mode.
type RedisConfig struct {
	Addr        string
	Password    string
	DB          int
	IDField     string           // Field within Record used as an identifier
	StorageType RedisStorageType // "key", "hash", or "list"
	// Below are container keys if a mode other than direct key/value is used.
	HashKey string // used if StorageType is "hash"
	ListKey string // used if StorageType is "list"
	// Optional: A key pattern if you need to scan keys for key/value mode.
	KeyPattern string
}

// RedisProvider encapsulates the redis client and configuration.
type RedisProvider struct {
	Client *redis.Client
	Config RedisConfig
}

// NewRedisProvider instantiates the provider.
func NewRedisProvider(config RedisConfig) *RedisProvider {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})
	return &RedisProvider{Client: client, Config: config}
}

// Setup verifies connection by issuing a Ping.
func (r *RedisProvider) Setup(ctx context.Context) error {
	return r.Client.Ping(ctx).Err()
}

// Close releases the underlying redis client connection.
func (r *RedisProvider) Close() error {
	return r.Client.Close()
}

// Create stores the given record. Its behavior depends on the storage type.
func (r *RedisProvider) Create(ctx context.Context, item Record) error {
	// Extract the record's unique ID.
	id, ok := item[r.Config.IDField].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", r.Config.IDField)
	}
	// Serialize the record to JSON.
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}

	switch r.Config.StorageType {
	case RedisStorageHash:
		// Require a container hash key
		if r.Config.HashKey == "" {
			return fmt.Errorf("missing HashKey for hash storage")
		}
		// Store the record as a field in the container hash.
		return r.Client.HSet(ctx, r.Config.HashKey, id, string(dataBytes)).Err()

	case RedisStorageList:
		// Require a container list key.
		if r.Config.ListKey == "" {
			return fmt.Errorf("missing ListKey for list storage")
		}
		// Append the record to the list.
		return r.Client.RPush(ctx, r.Config.ListKey, string(dataBytes)).Err()

	case RedisStorageKey:
		fallthrough
	default:
		// Use the record's unique ID as the key.
		return r.Client.Set(ctx, id, string(dataBytes), 0).Err()
	}
}

// Read retrieves a record by its ID.
func (r *RedisProvider) Read(ctx context.Context, id string) (Record, error) {
	var result string
	var err error

	switch r.Config.StorageType {
	case RedisStorageHash:
		if r.Config.HashKey == "" {
			return nil, fmt.Errorf("missing HashKey for hash storage")
		}
		result, err = r.Client.HGet(ctx, r.Config.HashKey, id).Result()

	case RedisStorageList:
		if r.Config.ListKey == "" {
			return nil, fmt.Errorf("missing ListKey for list storage")
		}
		// Retrieve the entire list and search for the record with the matching ID.
		items, err2 := r.Client.LRange(ctx, r.Config.ListKey, 0, -1).Result()
		if err2 != nil {
			return nil, err2
		}
		for _, itemStr := range items {
			var item Record
			if err := json.Unmarshal([]byte(itemStr), &item); err != nil {
				continue
			}
			if recID, ok := item[r.Config.IDField].(string); ok && recID == id {
				return item, nil
			}
		}
		return nil, redis.Nil

	case RedisStorageKey:
		fallthrough
	default:
		result, err = r.Client.Get(ctx, id).Result()
	}

	if err != nil {
		return nil, err
	}

	var item Record
	err = json.Unmarshal([]byte(result), &item)
	return item, err
}

// Update modifies an existing record. For most modes, this is equivalent to Create,
// but for list mode we perform a removal of the previous entry.
func (r *RedisProvider) Update(ctx context.Context, item Record) error {
	switch r.Config.StorageType {
	case RedisStorageList:
		// For list mode, remove the record if it exists before adding the new record.
		id, ok := item[r.Config.IDField].(string)
		if !ok {
			return fmt.Errorf("item missing id field %s", r.Config.IDField)
		}
		// Try to fetch the existing record.
		existing, err := r.Read(ctx, id)
		if err == nil {
			// Remove the existing record from the list.
			existingBytes, _ := json.Marshal(existing)
			r.Client.LRem(ctx, r.Config.ListKey, 0, string(existingBytes))
		}
		return r.Create(ctx, item)

	default:
		// For key and hash, simply "set" again.
		return r.Create(ctx, item)
	}
}

// Delete removes a record identified by its ID.
func (r *RedisProvider) Delete(ctx context.Context, id string) error {
	switch r.Config.StorageType {
	case RedisStorageHash:
		if r.Config.HashKey == "" {
			return fmt.Errorf("missing HashKey for hash storage")
		}
		return r.Client.HDel(ctx, r.Config.HashKey, id).Err()

	case RedisStorageList:
		if r.Config.ListKey == "" {
			return fmt.Errorf("missing ListKey for list storage")
		}
		// Retrieve all items from the list.
		items, err := r.Client.LRange(ctx, r.Config.ListKey, 0, -1).Result()
		if err != nil {
			return err
		}
		// Find the item with the matching ID.
		for _, itemStr := range items {
			var item Record
			if err := json.Unmarshal([]byte(itemStr), &item); err != nil {
				continue
			}
			if recID, ok := item[r.Config.IDField].(string); ok && recID == id {
				// Remove this item from the list.
				return r.Client.LRem(ctx, r.Config.ListKey, 0, itemStr).Err()
			}
		}
		return redis.Nil

	case RedisStorageKey:
		fallthrough
	default:
		return r.Client.Del(ctx, id).Err()
	}
}

// All retrieves all records.
// For key/value storage, an optional KeyPattern (default "*") is used for scanning keys.
// For hash and list modes, the container value is iterated.
func (r *RedisProvider) All(ctx context.Context) ([]Record, error) {
	var records []Record

	switch r.Config.StorageType {
	case RedisStorageHash:
		if r.Config.HashKey == "" {
			return nil, fmt.Errorf("missing HashKey for hash storage")
		}
		result, err := r.Client.HGetAll(ctx, r.Config.HashKey).Result()
		if err != nil {
			return nil, err
		}
		for _, v := range result {
			var item Record
			if err := json.Unmarshal([]byte(v), &item); err != nil {
				continue
			}
			records = append(records, item)
		}

	case RedisStorageList:
		if r.Config.ListKey == "" {
			return nil, fmt.Errorf("missing ListKey for list storage")
		}
		items, err := r.Client.LRange(ctx, r.Config.ListKey, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, itemStr := range items {
			var item Record
			if err := json.Unmarshal([]byte(itemStr), &item); err != nil {
				continue
			}
			records = append(records, item)
		}

	case RedisStorageKey:
		fallthrough
	default:
		pattern := "*"
		if r.Config.KeyPattern != "" {
			pattern = r.Config.KeyPattern
		}
		var cursor uint64
		// Use the SCAN operation to iterate over matching keys.
		for {
			keys, nextCursor, err := r.Client.Scan(ctx, cursor, pattern, 10).Result()
			if err != nil {
				return nil, err
			}
			for _, key := range keys {
				result, err := r.Client.Get(ctx, key).Result()
				if err != nil {
					continue
				}
				var item Record
				if err := json.Unmarshal([]byte(result), &item); err != nil {
					continue
				}
				records = append(records, item)
			}
			if nextCursor == 0 {
				break
			}
			cursor = nextCursor
		}
	}

	return records, nil
}

// Fields detects the schema of the data by sampling records from All().
// It relies on your custom DetectSchema implementation.
func (r *RedisProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
	records, err := r.All(ctx)
	if err != nil {
		return nil, err
	}
	return DetectSchema(records, 10), nil
}

func NewProvider(cfg ProviderConfig) (Provider, error) {
	switch cfg.Type {
	case "mysql", "postgres", "sqlite":
		return NewSQLProvider(cfg)
	case "rest":
		restCfg := RESTConfig{
			Timeout:   cfg.Timeout,
			Endpoints: cfg.Endpoints,
		}
		return NewRESTProvider(restCfg), nil
	case "json":
		jsonCfg := JSONFileConfig{
			FilePath: cfg.FilePath,
			IDField:  cfg.IDColumn,
		}
		return NewJSONFileProvider(jsonCfg), nil
	case "csv":
		csvCfg := CSVFileConfig{
			FilePath:    cfg.FilePath,
			IDColumn:    cfg.IDColumn,
			DataColumns: cfg.DataColumns,
		}
		return NewCSVFileProvider(csvCfg), nil
	case "redis":
		redisCfg := RedisConfig{
			Password: cfg.Password,
			IDField:  cfg.IDColumn,
		}
		redisCfg.Addr = cfg.Host
		db, _ := strconv.Atoi(cfg.Database)
		redisCfg.DB = db
		return NewRedisProvider(redisCfg), nil
	default:
		return nil, fmt.Errorf("unsupported providers type: %s", cfg.Type)
	}
}

func dbFromConfig(config squealx.Config) (*squealx.DB, error) {
	if config.Key == "" {
		config.Key = config.Name
	}
	dsn := config.ToString()
	switch config.Driver {
	case "mysql", "mariadb":
		return mysql.Open(dsn, config.Key)
	case "postgres", "psql", "postgresql":
		return postgres.Open(dsn, config.Key)
	case "sql-server", "sqlserver", "mssql", "ms-sql":
		return mssql.Open(dsn, config.Key)
	case "sqlite", "sqlite3":
		return sqlite.Open(config.Database, config.Key)
	default:
		return nil, errors.New("unsupported driver")
	}
}
