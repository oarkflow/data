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

	"github.com/oarkflow/data/utils"
)

type Field struct {
	Name      string
	DataType  string
	IsPrimary bool
	Nullable  bool
}

type Provider interface {
	Setup(ctx context.Context) error
	Create(ctx context.Context, item utils.Record) error
	Read(ctx context.Context, id string) (utils.Record, error)
	Update(ctx context.Context, item utils.Record) error
	Delete(ctx context.Context, id string) error
	All(ctx context.Context) ([]utils.Record, error)
	Close() error
}

type StreamingProvider interface {
	Provider
	Stream(ctx context.Context) (<-chan utils.Record, <-chan error)
}

type ProviderConfig struct {
	Type string

	squealx.Config
	TableName    string
	IDColumn     string
	DataColumn   string
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

func (s *SQLProvider) Create(ctx context.Context, item utils.Record) error {
	return s.db.Create(ctx, &item)
}

func (s *SQLProvider) Read(ctx context.Context, id string) (utils.Record, error) {
	return s.db.First(ctx, map[string]any{
		s.Config.IDColumn: id,
	})
}

func (s *SQLProvider) Update(ctx context.Context, item utils.Record) error {
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

func (s *SQLProvider) All(ctx context.Context) ([]utils.Record, error) {
	return s.db.All(ctx)
}

func (s *SQLProvider) Stream(ctx context.Context) (<-chan utils.Record, <-chan error) {
	out := make(chan utils.Record)
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

func (r *RESTProvider) Create(ctx context.Context, item utils.Record) error {
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

func (r *RESTProvider) Read(ctx context.Context, id string) (utils.Record, error) {
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
	var item utils.Record
	err = json.Unmarshal(dataBytes, &item)
	return item, err
}

func (r *RESTProvider) Update(ctx context.Context, item utils.Record) error {
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

func (r *RESTProvider) All(ctx context.Context) ([]utils.Record, error) {
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
	var items []utils.Record
	if err := json.Unmarshal(dataBytes, &items); err != nil {
		return nil, err
	}
	return items, nil
}

type JSONFileConfig struct {
	FilePath string
	IDField  string
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

func (p *JSONFileProvider) readAll() ([]utils.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var items []utils.Record
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

func (p *JSONFileProvider) writeAll(items []utils.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := json.Marshal(items)
	if err != nil {
		return err
	}
	return os.WriteFile(p.Config.FilePath, data, 0644)
}

func (p *JSONFileProvider) Create(_ context.Context, item utils.Record) error {
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

func (p *JSONFileProvider) Read(_ context.Context, id string) (utils.Record, error) {
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

func (p *JSONFileProvider) Update(_ context.Context, item utils.Record) error {
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
	var newItems []utils.Record
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

func (p *JSONFileProvider) All(_ context.Context) ([]utils.Record, error) {
	return p.readAll()
}

func (p *JSONFileProvider) Stream(ctx context.Context) (<-chan utils.Record, <-chan error) {
	out := make(chan utils.Record)
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
	FilePath   string
	IDColumn   string
	DataColumn string
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
		return writer.Write([]string{p.Config.IDColumn, p.Config.DataColumn})
	}
	return nil
}

func (p *CSVFileProvider) readAll() ([]utils.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var items []utils.Record
	if _, err := os.Stat(p.Config.FilePath); os.IsNotExist(err) {
		return items, nil
	}
	f, err := os.Open(p.Config.FilePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 1 {
		return items, nil
	}
	for i, record := range records {
		if i == 0 {
			continue
		}
		if len(record) < 2 {
			continue
		}
		var item utils.Record
		if err := json.Unmarshal([]byte(record[1]), &item); err != nil {
			continue
		}
		item[p.Config.IDColumn] = record[0]
		items = append(items, item)
	}
	return items, nil
}

func (p *CSVFileProvider) writeAll(items []utils.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, err := os.Create(p.Config.FilePath)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	writer := csv.NewWriter(f)
	defer writer.Flush()

	if err := writer.Write([]string{p.Config.IDColumn, p.Config.DataColumn}); err != nil {
		return err
	}
	for _, item := range items {
		id, _ := item[p.Config.IDColumn].(string)
		dataBytes, err := json.Marshal(item)
		if err != nil {
			continue
		}
		if err := writer.Write([]string{id, string(dataBytes)}); err != nil {
			return err
		}
	}
	return nil
}

func (p *CSVFileProvider) Create(_ context.Context, item utils.Record) error {
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

func (p *CSVFileProvider) Read(_ context.Context, id string) (utils.Record, error) {
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

func (p *CSVFileProvider) Update(_ context.Context, item utils.Record) error {
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
	var newItems []utils.Record
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

func (p *CSVFileProvider) All(_ context.Context) ([]utils.Record, error) {
	return p.readAll()
}

func (p *CSVFileProvider) Stream(ctx context.Context) (<-chan utils.Record, <-chan error) {
	out := make(chan utils.Record)
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

func (r *RedisProvider) Create(ctx context.Context, item utils.Record) error {
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

func (r *RedisProvider) Read(ctx context.Context, id string) (utils.Record, error) {
	result, err := r.Client.Get(ctx, id).Result()
	if err != nil {
		return nil, err
	}
	var item utils.Record
	err = json.Unmarshal([]byte(result), &item)
	return item, err
}

func (r *RedisProvider) Update(ctx context.Context, item utils.Record) error {
	return r.Create(ctx, item)
}

func (r *RedisProvider) Delete(ctx context.Context, id string) error {
	return r.Client.Del(ctx, id).Err()
}

func (r *RedisProvider) All(ctx context.Context) ([]utils.Record, error) {
	var cursor uint64
	var items []utils.Record
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
			var item utils.Record
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
			FilePath:   cfg.FilePath,
			IDColumn:   cfg.IDColumn,
			DataColumn: cfg.DataColumn,
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
