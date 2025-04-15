package data

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	Fields(ctx context.Context) (map[string]FieldSchema, error)
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
	Host        string
	Password    string
	Database    string
	KeyPattern  string
	Queries     Queries
}

func ValidateProviderConfig(cfg *ProviderConfig) error {
	if cfg.Type == "" {
		return errors.New("provider type is required")
	}
	switch cfg.Type {
	case "mysql", "postgres", "sqlite":
		if cfg.TableName == "" || cfg.IDColumn == "" {
			return fmt.Errorf("table name and id column required for SQL provider")
		}
	case "json", "csv":
		if cfg.FilePath == "" || cfg.IDColumn == "" {
			return fmt.Errorf("file path and id column are required for file provider")
		}
	case "rest":

		if cfg.Endpoints.Read.URL == "" && cfg.Endpoints.Create.URL == "" {
			return fmt.Errorf("at least one REST endpoint URL must be provided")
		}
	case "redis":
		if cfg.Host == "" || cfg.IDColumn == "" {
			return fmt.Errorf("redis host and id column are required")
		}
	}
	return nil
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
	if err := ValidateProviderConfig(&config); err != nil {
		return nil, err
	}
	db, err := dbFromConfig(config.Config)
	if err != nil {
		return nil, fmt.Errorf("dbFromConfig error: %w", err)
	}
	repo := squealx.New[map[string]any](db, config.TableName, config.IDColumn)
	p := &SQLProvider{db: repo, Config: config}

	if err := p.Setup(context.Background()); err != nil {
		return nil, fmt.Errorf("setup error: %w", err)
	}
	return p, nil
}

func (s *SQLProvider) Close() error {
	return s.db.GetDB().Close()
}

func (s *SQLProvider) Setup(ctx context.Context) error {
	if err := s.db.GetDB().Ping(); err != nil {
		return fmt.Errorf("ping error: %w", err)
	}
	return nil
}

func (s *SQLProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
	fieldList := make(map[string]FieldSchema)
	fields, err := s.db.GetDB().GetTableFields(s.Config.TableName, s.Config.Database)
	if err != nil {
		return nil, fmt.Errorf("GetTableFields error: %w", err)
	}
	for _, field := range fields {
		schema := FieldSchema{
			FieldName:       field.Name,
			DataType:        field.DataType,
			MaxStringLength: field.Length,
		}
		if strings.ToUpper(field.IsNullable) == "YES" {
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
			return nil, fmt.Errorf("raw query read error: %w", err)
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
			return fmt.Errorf("raw query update error: %w", err)
		}
		if len(records) == 0 {
			return fmt.Errorf("not found")
		}
		return nil
	}
	err := s.db.Update(ctx, &item, filter)
	return squealx.CanError(err, false)
}

func (s *SQLProvider) Delete(ctx context.Context, id string) error {
	filter := map[string]any{
		s.Config.IDColumn: id,
	}
	if s.Config.Queries.QueryDelete != "" {
		records, err := s.db.Raw(ctx, s.Config.Queries.QueryDelete, filter)
		if err != nil {
			return fmt.Errorf("raw query delete error: %w", err)
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
			errCh <- fmt.Errorf("streaming error: %w", err)
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

type EndpointConfig struct {
	URL     string
	Method  string
	Headers map[string]string
	IDField string
	DataKey string
}

type EndpointsConfig struct {
	Setup  EndpointConfig
	Create EndpointConfig
	Read   EndpointConfig
	Update EndpointConfig
	Delete EndpointConfig
	All    EndpointConfig
}

type RESTConfig struct {
	Timeout   time.Duration
	Endpoints EndpointsConfig
}

type RESTProvider struct {
	client    *http.Client
	endpoints EndpointsConfig
	retries   int
	backoff   time.Duration
}

func NewRESTProvider(config RESTConfig) *RESTProvider {
	return &RESTProvider{
		client: &http.Client{
			Timeout: config.Timeout,
		},
		endpoints: config.Endpoints,
		retries:   3,
		backoff:   500 * time.Millisecond,
	}
}

func applyHeaders(req *http.Request, headers map[string]string) {
	for key, value := range headers {
		req.Header.Set(key, value)
	}
}

func doWithRetry(ctx context.Context, attempts int, delay time.Duration, operation func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = operation()
		if err == nil {
			return nil
		}
		log.Printf("operation failed (attempt %d/%d): %v", i+1, attempts, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay * time.Duration(1<<i)):
		}
	}
	return fmt.Errorf("operation failed after %d attempts: %w", attempts, err)
}

func (r *RESTProvider) Setup(ctx context.Context) error {
	return nil
}

func (r *RESTProvider) Create(ctx context.Context, item Record) error {
	ep := r.endpoints.Create
	if ep.Method == "" {
		ep.Method = "POST"
	}
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}
	op := func() error {
		req, err := http.NewRequestWithContext(ctx, ep.Method, ep.URL, bytes.NewBuffer(dataBytes))
		if err != nil {
			return fmt.Errorf("new request error: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		applyHeaders(req, ep.Headers)
		resp, err := r.client.Do(req)
		if err != nil {
			return fmt.Errorf("http do error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to create item, status: %s, body: %s", resp.Status, string(bodyBytes))
		}
		return nil
	}
	return doWithRetry(ctx, r.retries, r.backoff, op)
}

func (r *RESTProvider) Read(ctx context.Context, id string) (Record, error) {
	ep := r.endpoints.Read
	if ep.Method == "" {
		ep.Method = "GET"
	}

	encodedID := url.QueryEscape(id)
	readURL := fmt.Sprintf(ep.URL, encodedID)
	var result Record
	op := func() error {
		req, err := http.NewRequestWithContext(ctx, ep.Method, readURL, nil)
		if err != nil {
			return fmt.Errorf("new request error: %w", err)
		}
		applyHeaders(req, ep.Headers)
		resp, err := r.client.Do(req)
		if err != nil {
			return fmt.Errorf("http do error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to read item, status: %s, body: %s", resp.Status, string(bodyBytes))
		}
		dataBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read body error: %w", err)
		}
		isArray, err := CheckJSON(dataBytes)
		if err != nil {
			return fmt.Errorf("check json error: %w", err)
		}
		if isArray {
			var items []Record
			if err := json.Unmarshal(dataBytes, &items); err != nil {
				return fmt.Errorf("json unmarshal array error: %w", err)
			}
			if len(items) > 0 {
				result = items[0]
				return nil
			}
			for _, item := range items {
				if item[ep.IDField] == id {
					result = item
					return nil
				}
			}
			return fmt.Errorf("item not found")
		}
		if ep.DataKey != "" {
			dataBytes, _, _, err = jsonparser.Get(dataBytes, ep.DataKey)
			if err != nil {
				return fmt.Errorf("jsonparser error: %w", err)
			}
		}
		var item Record
		if err := json.Unmarshal(dataBytes, &item); err != nil {
			return fmt.Errorf("json unmarshal error: %w", err)
		}
		result = item
		return nil
	}
	if err := doWithRetry(ctx, r.retries, r.backoff, op); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *RESTProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
	records, err := r.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("all records error: %w", err)
	}
	return DetectSchema(records, 10), nil
}

func (r *RESTProvider) Update(ctx context.Context, item Record) error {
	ep := r.endpoints.Update
	if ep.Method == "" {
		ep.Method = "PUT"
	}
	dataBytes, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	id, ok := item[ep.IDField].(string)
	if !ok {
		return fmt.Errorf("item missing id field %s", ep.IDField)
	}
	encodedID := url.QueryEscape(id)
	updateURL := fmt.Sprintf(ep.URL, encodedID)
	op := func() error {
		req, err := http.NewRequestWithContext(ctx, ep.Method, updateURL, bytes.NewBuffer(dataBytes))
		if err != nil {
			return fmt.Errorf("new request error: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		applyHeaders(req, ep.Headers)
		resp, err := r.client.Do(req)
		if err != nil {
			return fmt.Errorf("http do error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to update item, status: %s, body: %s", resp.Status, string(bodyBytes))
		}
		return nil
	}
	return doWithRetry(ctx, r.retries, r.backoff, op)
}

func (r *RESTProvider) Delete(ctx context.Context, id string) error {
	ep := r.endpoints.Delete
	if ep.Method == "" {
		ep.Method = "DELETE"
	}
	encodedID := url.QueryEscape(id)
	deleteURL := fmt.Sprintf(ep.URL, encodedID)
	op := func() error {
		req, err := http.NewRequestWithContext(ctx, ep.Method, deleteURL, nil)
		if err != nil {
			return fmt.Errorf("new request error: %w", err)
		}
		applyHeaders(req, ep.Headers)
		resp, err := r.client.Do(req)
		if err != nil {
			return fmt.Errorf("http do error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to delete item, status: %s, body: %s", resp.Status, string(bodyBytes))
		}
		return nil
	}
	return doWithRetry(ctx, r.retries, r.backoff, op)
}

func (r *RESTProvider) All(ctx context.Context) ([]Record, error) {
	ep := r.endpoints.All
	if ep.Method == "" {
		ep.Method = "GET"
	}
	var items []Record
	op := func() error {
		req, err := http.NewRequestWithContext(ctx, ep.Method, ep.URL, nil)
		if err != nil {
			return fmt.Errorf("new request error: %w", err)
		}
		applyHeaders(req, ep.Headers)
		resp, err := r.client.Do(req)
		if err != nil {
			return fmt.Errorf("http do error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to get all items, status: %s, body: %s", resp.Status, string(bodyBytes))
		}
		dataBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read body error: %w", err)
		}
		isArray, err := CheckJSON(dataBytes)
		if err != nil {
			return fmt.Errorf("check json error: %w", err)
		}
		if !isArray && ep.DataKey != "" {
			dataBytes, _, _, err = jsonparser.Get(dataBytes, ep.DataKey)
			if err != nil {
				return fmt.Errorf("jsonparser error: %w", err)
			}
		}
		if err := json.Unmarshal(dataBytes, &items); err != nil {
			return fmt.Errorf("json unmarshal error: %w", err)
		}
		return nil
	}
	if err := doWithRetry(ctx, r.retries, r.backoff, op); err != nil {
		return nil, err
	}
	return items, nil
}

func (r *RESTProvider) Close() error {
	r.client.CloseIdleConnections()
	return nil
}

func (r *RESTProvider) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		records, err := r.All(ctx)
		if err != nil {
			errCh <- fmt.Errorf("stream error: %w", err)
			return
		}
		for _, item := range records {
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

type FileConfig struct {
	FilePath        string
	IDColumn        string
	DataColumns     []string
	TombstoneMarker string
	Newline         string
	Delimiter       rune
}

type JSONFileProvider struct {
	Config FileConfig
	mu     sync.Mutex
}

func NewJSONFileProvider(config FileConfig) *JSONFileProvider {
	return &JSONFileProvider{Config: config}
}

func (p *JSONFileProvider) Close() error {
	return nil
}

func (p *JSONFileProvider) Setup(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := os.Stat(p.Config.FilePath); os.IsNotExist(err) {
		if err := os.WriteFile(p.Config.FilePath, []byte("[]"), 0644); err != nil {
			return fmt.Errorf("write file error: %w", err)
		}
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
		return nil, fmt.Errorf("read file error: %w", err)
	}
	if len(data) == 0 {
		return items, nil
	}
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %w", err)
	}
	return items, nil
}

func (p *JSONFileProvider) writeAll(items []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := json.Marshal(items)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}
	if err := os.WriteFile(p.Config.FilePath, data, 0644); err != nil {
		return fmt.Errorf("write file error: %w", err)
	}
	return nil
}

func (p *JSONFileProvider) Create(_ context.Context, item Record) error {
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

func (p *JSONFileProvider) Read(_ context.Context, id string) (Record, error) {
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

func (p *JSONFileProvider) Update(_ context.Context, item Record) error {
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

func (p *JSONFileProvider) Delete(_ context.Context, id string) error {
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

func (p *JSONFileProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
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

func (p *JSONFileProvider) BulkCreate(_ context.Context, items []Record) error {
	current, err := p.readAll()
	if err != nil {
		return err
	}
	existingIDs := make(map[string]bool)
	for _, it := range current {
		if id, ok := it[p.Config.IDColumn].(string); ok {
			existingIDs[id] = true
		}
	}
	for _, item := range items {
		id, ok := item[p.Config.IDColumn].(string)
		if !ok {
			return fmt.Errorf("item missing id field %s", p.Config.IDColumn)
		}
		if existingIDs[id] {
			return fmt.Errorf("item with id %s already exists", id)
		}
		current = append(current, item)
	}
	return p.writeAll(current)
}

type CSVFileProvider struct {
	Config FileConfig
	mu     sync.Mutex
}

func NewCSVFileProvider(config FileConfig) *CSVFileProvider {
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
			return fmt.Errorf("create file error: %w", err)
		}
		defer f.Close()
		writer := csv.NewWriter(f)
		defer writer.Flush()
		cols := []string{p.Config.IDColumn}
		cols = append(cols, p.Config.DataColumns...)
		if err := writer.Write(cols); err != nil {
			return fmt.Errorf("write header error: %w", err)
		}
	}
	return nil
}

func (p *CSVFileProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
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
		return nil, fmt.Errorf("open file error: %w", err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv error: %w", err)
	}
	if len(records) < 1 {
		return items, nil
	}
	header := records[0]
	for _, record := range records[1:] {
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
	switch v := val.(type) {
	case string:
		return v, true
	default:
		return fmt.Sprint(v), true
	}
}

func (p *CSVFileProvider) writeAll(items []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, err := os.Create(p.Config.FilePath)
	if err != nil {
		return fmt.Errorf("create file error: %w", err)
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	defer writer.Flush()
	header := []string{p.Config.IDColumn}
	header = append(header, p.Config.DataColumns...)
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("write header error: %w", err)
	}
	for _, item := range items {
		row := make([]string, len(header))
		if id, ok := item[p.Config.IDColumn].(string); ok {
			row[0] = id
		} else {
			row[0] = ""
		}
		for i, col := range p.Config.DataColumns {
			val, exists := item[col]
			if !exists {
				row[i+1] = ""
				continue
			}
			if s, ok := toString(val); ok {
				row[i+1] = s
			} else {
				row[i+1] = fmt.Sprintf("%v", val)
			}
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("write row error: %w", err)
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
	RedisStorageKey  RedisStorageType = "key"
	RedisStorageHash RedisStorageType = "hash"
	RedisStorageList RedisStorageType = "list"
)

type RedisConfig struct {
	Addr        string
	Password    string
	DB          int
	IDField     string
	StorageType RedisStorageType

	HashKey    string
	ListKey    string
	KeyPattern string
}

func ValidateRedisConfig(cfg *RedisConfig) error {
	if cfg.Addr == "" {
		return fmt.Errorf("redis address required")
	}
	if cfg.IDField == "" {
		return fmt.Errorf("id field required for redis")
	}
	return nil
}

type RedisProvider struct {
	Client *redis.Client
	Config RedisConfig
}

func NewRedisProvider(config RedisConfig) (*RedisProvider, error) {
	if err := ValidateRedisConfig(&config); err != nil {
		return nil, err
	}
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})
	return &RedisProvider{Client: client, Config: config}, nil
}

func (r *RedisProvider) Setup(ctx context.Context) error {
	if err := r.Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping error: %w", err)
	}
	return nil
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
		return fmt.Errorf("json marshal error: %w", err)
	}
	switch r.Config.StorageType {
	case RedisStorageHash:
		if r.Config.HashKey == "" {
			return fmt.Errorf("missing HashKey for hash storage")
		}
		return r.Client.HSet(ctx, r.Config.HashKey, id, string(dataBytes)).Err()
	case RedisStorageList:
		if r.Config.ListKey == "" {
			return fmt.Errorf("missing ListKey for list storage")
		}
		return r.Client.RPush(ctx, r.Config.ListKey, string(dataBytes)).Err()
	case RedisStorageKey:
		fallthrough
	default:
		return r.Client.Set(ctx, id, string(dataBytes), 0).Err()
	}
}

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
		return nil, fmt.Errorf("redis get error: %w", err)
	}
	var item Record
	if err := json.Unmarshal([]byte(result), &item); err != nil {
		return nil, fmt.Errorf("json unmarshal error: %w", err)
	}
	return item, nil
}

func (r *RedisProvider) Update(ctx context.Context, item Record) error {
	switch r.Config.StorageType {
	case RedisStorageList:
		id, ok := item[r.Config.IDField].(string)
		if !ok {
			return fmt.Errorf("item missing id field %s", r.Config.IDField)
		}
		existing, err := r.Read(ctx, id)
		if err == nil {
			existingBytes, _ := json.Marshal(existing)
			r.Client.LRem(ctx, r.Config.ListKey, 0, string(existingBytes))
		}
		return r.Create(ctx, item)
	default:
		return r.Create(ctx, item)
	}
}

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
		items, err := r.Client.LRange(ctx, r.Config.ListKey, 0, -1).Result()
		if err != nil {
			return err
		}
		for _, itemStr := range items {
			var item Record
			if err := json.Unmarshal([]byte(itemStr), &item); err != nil {
				continue
			}
			if recID, ok := item[r.Config.IDField].(string); ok && recID == id {
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

func (r *RedisProvider) Fields(ctx context.Context) (map[string]FieldSchema, error) {
	records, err := r.All(ctx)
	if err != nil {
		return nil, err
	}
	return DetectSchema(records, 10), nil
}

func (r *RedisProvider) Stream(ctx context.Context) (<-chan Record, <-chan error) {
	out := make(chan Record)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errCh)
		records, err := r.All(ctx)
		if err != nil {
			errCh <- fmt.Errorf("stream error: %w", err)
			return
		}
		for _, item := range records {
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
		jsonCfg := FileConfig{
			FilePath: cfg.FilePath,
			IDColumn: cfg.IDColumn,
		}
		return NewJSONFileProvider(jsonCfg), nil
	case "csv":
		csvCfg := FileConfig{
			FilePath:    cfg.FilePath,
			IDColumn:    cfg.IDColumn,
			DataColumns: cfg.DataColumns,
		}
		return NewCSVFileProvider(csvCfg), nil
	case "redis":
		db, _ := strconv.Atoi(cfg.Database)
		redisCfg := RedisConfig{
			Password:    cfg.Password,
			IDField:     cfg.IDColumn,
			Addr:        cfg.Host,
			DB:          db,
			KeyPattern:  cfg.KeyPattern,
			StorageType: RedisStorageKey,
		}
		return NewRedisProvider(redisCfg)
	default:
		return nil, fmt.Errorf("unsupported provider type: %s", cfg.Type)
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
