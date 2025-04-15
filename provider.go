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

	"github.com/go-redis/redis/v8"
	"github.com/oarkflow/json"
	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/mssql"
	"github.com/oarkflow/squealx/drivers/mysql"
	"github.com/oarkflow/squealx/drivers/postgres"
	"github.com/oarkflow/squealx/drivers/sqlite"
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
	Delete(ctx context.Context, id string) error
	All(ctx context.Context) ([]Record, error)
	Close() error
}

type StreamingProvider interface {
	Provider
	Stream(ctx context.Context) (<-chan Record, <-chan error)
}

type ProviderConfig struct {
	Type string

	squealx.Config
	TableName    string
	IDColumn     string
	DataColumns  []string
	BaseURL      string
	Timeout      time.Duration
	ResourcePath string
	FilePath     string
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
	return nil
}

func (s *SQLProvider) Create(ctx context.Context, item Record) error {
	return s.db.Create(ctx, &item)
}

func (s *SQLProvider) Read(ctx context.Context, id string) (Record, error) {
	return s.db.First(ctx, map[string]any{
		s.Config.IDColumn: id,
	})
}

func (s *SQLProvider) Update(ctx context.Context, item Record) error {
	id, ok := item[s.Config.IDColumn].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", s.Config.IDColumn)
	}
	return s.db.Update(ctx, &item, map[string]any{
		s.Config.IDColumn: id,
	})
}

func (s *SQLProvider) Delete(ctx context.Context, id string) error {
	return s.db.Delete(ctx, map[string]any{
		s.Config.IDColumn: id,
	})
}

func (s *SQLProvider) All(ctx context.Context) ([]Record, error) {
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

type RESTConfig struct {
	BaseURL      string
	Timeout      time.Duration
	ResourcePath string
	IDField      string
}

type RESTProvider struct {
	baseURL      string
	client       *http.Client
	resourcePath string
	IdField      string
}

func NewRESTProvider(config RESTConfig) *RESTProvider {
	return &RESTProvider{
		baseURL:      config.BaseURL,
		client:       &http.Client{Timeout: config.Timeout},
		resourcePath: config.ResourcePath,
		IdField:      config.IDField,
	}
}

func (r *RESTProvider) Setup(ctx context.Context) error {
	url := fmt.Sprintf("%s/%s", r.baseURL, r.resourcePath)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("REST endpoint not available: %s", resp.Status)
	}
	return nil
}

func (r *RESTProvider) Close() error {
	r.client.CloseIdleConnections()
	return nil
}

func (r *RESTProvider) Create(ctx context.Context, item Record) error {
	url := fmt.Sprintf("%s/%s", r.baseURL, r.resourcePath)
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(dataBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("failed to create item, status: %s", resp.Status)
	}
	return nil
}

func (r *RESTProvider) Read(ctx context.Context, id string) (Record, error) {
	url := fmt.Sprintf("%s/%s/%s", r.baseURL, r.resourcePath, id)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to read item, status: %s", resp.Status)
	}
	dataBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var item Record
	err = json.Unmarshal(dataBytes, &item)
	return item, err
}

func (r *RESTProvider) Update(ctx context.Context, item Record) error {
	id, ok := item[r.IdField].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", r.IdField)
	}
	url := fmt.Sprintf("%s/%s/%s", r.baseURL, r.resourcePath, id)
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewBuffer(dataBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to update item, status: %s", resp.Status)
	}
	return nil
}

func (r *RESTProvider) Delete(ctx context.Context, id string) error {
	url := fmt.Sprintf("%s/%s/%s", r.baseURL, r.resourcePath, id)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete item, status: %s", resp.Status)
	}
	return nil
}

func (r *RESTProvider) All(ctx context.Context) ([]Record, error) {
	url := fmt.Sprintf("%s/%s", r.baseURL, r.resourcePath)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get all items, status: %s", resp.Status)
	}
	dataBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var items []Record
	if err := json.Unmarshal(dataBytes, &items); err != nil {
		return nil, err
	}
	return items, nil
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

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	IDField  string
}

type RedisProvider struct {
	Client *redis.Client
	Config RedisConfig
}

func NewRedisProvider(config RedisConfig) *RedisProvider {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})
	return &RedisProvider{Client: client, Config: config}
}

func (r *RedisProvider) Setup(ctx context.Context) error {
	return r.Client.Ping(ctx).Err()
}

func (r *RedisProvider) Close() error {
	return r.Client.Close()
}

func (r *RedisProvider) Create(ctx context.Context, item Record) error {
	id, ok := item[r.Config.IDField].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", r.Config.IDField)
	}
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return r.Client.Set(ctx, id, string(dataBytes), 0).Err()
}

func (r *RedisProvider) Read(ctx context.Context, id string) (Record, error) {
	result, err := r.Client.Get(ctx, id).Result()
	if err != nil {
		return nil, err
	}
	var item Record
	err = json.Unmarshal([]byte(result), &item)
	return item, err
}

func (r *RedisProvider) Update(ctx context.Context, item Record) error {
	return r.Create(ctx, item)
}

func (r *RedisProvider) Delete(ctx context.Context, id string) error {
	return r.Client.Del(ctx, id).Err()
}

func (r *RedisProvider) All(ctx context.Context) ([]Record, error) {
	var cursor uint64
	var items []Record
	for {
		keys, nextCursor, err := r.Client.Scan(ctx, cursor, "*", 10).Result()
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
			items = append(items, item)
		}
		if nextCursor == 0 {
			break
		}
		cursor = nextCursor
	}
	return items, nil
}

func NewProvider(cfg ProviderConfig) (Provider, error) {
	switch cfg.Type {
	case "mysql", "postgres", "sqlite":
		return NewSQLProvider(cfg)
	case "rest":
		restCfg := RESTConfig{
			BaseURL:      cfg.BaseURL,
			Timeout:      cfg.Timeout,
			ResourcePath: cfg.ResourcePath,
			IDField:      cfg.IDColumn,
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
