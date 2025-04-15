package data

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/date"
	"github.com/oarkflow/json"
)

// Data type mappings for different drivers.
var (
	mysqlDataTypes = map[string]string{
		"int64":       "BIGINT",
		"int32":       "INT",
		"uint32":      "INT UNSIGNED",
		"float64":     "DOUBLE",
		"bool":        "BOOLEAN",
		"time":        "DATETIME",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "VARCHAR(255)",
		"json":        "JSON",
		"bytes":       "BLOB",
		"slice":       "JSON",
		"map":         "JSON",
		"struct":      "JSON",
		"json.Number": "BIGINT",
		"uuid":        "CHAR(36)",
	}

	postgresDataTypes = map[string]string{
		"int64":       "BIGINT",
		"int32":       "INTEGER",
		"uint32":      "INTEGER",
		"float64":     "DOUBLE PRECISION",
		"bool":        "BOOLEAN",
		"time":        "TIMESTAMP",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "VARCHAR(255)",
		"json":        "JSONB",
		"bytes":       "bytea",
		"slice":       "JSONB",
		"map":         "JSONB",
		"struct":      "JSONB",
		"json.Number": "BIGINT",
		"uuid":        "UUID",
	}

	sqliteDataTypes = map[string]string{
		"int64":       "INTEGER",
		"int32":       "INTEGER",
		"uint32":      "INTEGER",
		"float64":     "REAL",
		"bool":        "NUMERIC",
		"time":        "DATETIME",
		"date":        "DATE",
		"complex":     "TEXT",
		"string":      "TEXT",
		"json":        "JSON",
		"bytes":       "BLOB",
		"slice":       "JSON",
		"map":         "JSON",
		"struct":      "JSON",
		"json.Number": "INTEGER",
		"uuid":        "TEXT",
	}
)

var globalWorkerCount = 4

// FieldSchema holds the detected schema for a field.
type FieldSchema struct {
	FieldName       string  `json:"field_name"`
	DataType        string  `json:"data_type"`
	IsNullable      bool    `json:"is_nullable"`
	IsPrimaryKey    bool    `json:"is_primary_key"`
	MaxStringLength int     `json:"max_string_length"`
	MinValue        float64 `json:"min_value,omitempty"`
	MaxValue        float64 `json:"max_value,omitempty"`
}

// FieldStats aggregates statistics and heuristics for a field.
type FieldStats struct {
	countNonNull         int
	nullable             bool
	uniqueValues         map[string]bool
	duplicateFound       bool
	typeCounts           map[string]int
	hasNumeric           bool
	numericAllIntegral   bool
	totalStringCount     int
	stringAsTimeCount    int
	stringAsComplexCount int
	stringAsIntCount     int
	stringAsFloatCount   int
	maxStringLength      int
	minNumeric           float64
	maxNumeric           float64
	numericInitialized   bool
	stringAsBoolCount    int
	stringAsDateCount    int
	stringAsJsonCount    int
	stringAsUUIDCount    int
	mu                   sync.Mutex // protect concurrent updates
}

// Logging helper for warnings and errors.
func warnf(format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

func dereference(value any) any {
	for value != nil && reflect.TypeOf(value).Kind() == reflect.Ptr {
		v := reflect.ValueOf(value)
		if v.IsNil() {
			return nil
		}
		value = v.Elem().Interface()
	}
	return value
}

func fastString(value any) string {
	if value == nil {
		return "nil"
	}
	switch v := value.(type) {
	case string:
		return v
	case int:
		return strconv.Itoa(v)
	case int8, int16, int32, int64:
		return strconv.FormatInt(reflect.ValueOf(v).Int(), 10)
	case uint, uint8, uint16, uint32, uint64:
		return strconv.FormatUint(reflect.ValueOf(v).Uint(), 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", value)
	}
}

func isStringTime(s string) bool {
	_, err := date.Parse(s)
	return err == nil
}

func isStringComplex(s string) bool {
	_, err := strconv.ParseComplex(s, 128)
	return err == nil
}

func isStringDate(s string) bool {
	if len(s) == 10 {
		_, err := time.Parse("2006-01-02", s)
		return err == nil
	}
	return false
}

func isStringBool(s string) (bool, error) {
	lower := strings.ToLower(strings.TrimSpace(s))
	if lower == "true" || lower == "false" {
		return true, nil
	}
	return false, errors.New("not a bool")
}

func isJSONString(s string) bool {
	start, end := 0, len(s)-1
	for start <= end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end >= start && (s[end] == ' ' || s[end] == '\t' || s[end] == '\n' || s[end] == '\r') {
		end--
	}
	if start > end {
		return false
	}
	if (s[start] == '{' && s[end] == '}') || (s[start] == '[' && s[end] == ']') {
		var js interface{}
		if err := json.Unmarshal([]byte(s[start:end+1]), &js); err == nil {
			return true
		}
	}
	return false
}

func isStringUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	return true
}

func updateNumericRange(stats *FieldStats, num float64) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if !stats.numericInitialized {
		stats.minNumeric = num
		stats.maxNumeric = num
		stats.numericInitialized = true
	} else {
		if num < stats.minNumeric {
			stats.minNumeric = num
		}
		if num > stats.maxNumeric {
			stats.maxNumeric = num
		}
	}
}

func updateSchema(schema map[string]FieldSchema, field, finalType string, stats *FieldStats) {
	minVal, maxVal := 0.0, 0.0
	if stats.hasNumeric && stats.numericInitialized {
		minVal = stats.minNumeric
		maxVal = stats.maxNumeric
	}
	schema[field] = FieldSchema{
		FieldName:       field,
		DataType:        finalType,
		IsNullable:      stats.nullable,
		IsPrimaryKey:    false,
		MaxStringLength: stats.maxStringLength,
		MinValue:        minVal,
		MaxValue:        maxVal,
	}
}

func GetFields(data any) []string {
	fn := func(rec Record) []string {
		keys := maps.Keys(rec)
		var result []string
		for v := range keys {
			result = append(result, v)
		}
		return result
	}
	switch data := data.(type) {
	case Record:
		return fn(data)
	case []Record:
		if len(data) == 0 {
			return nil
		}
		return fn(data[0])
	default:
		return nil
	}
}

// DetectSchema processes the sample data concurrently to detect field schemas.
func DetectSchema(data []Record, sampleSize int) map[string]FieldSchema {
	schema := make(map[string]FieldSchema)
	fieldStats := make(map[string]*FieldStats)
	var mu sync.Mutex

	// Determine effective sample size.
	totalSize := len(data)
	if sampleSize > totalSize {
		sampleSize = totalSize
	}

	workerCount := globalWorkerCount
	rowsCh := make(chan Record, sampleSize)
	var wg sync.WaitGroup

	// Worker function to update fieldStats concurrently.
	worker := func() {
		defer wg.Done()
		for row := range rowsCh {
			for key, origValue := range row {
				value := dereference(origValue)
				mu.Lock()
				stats, exists := fieldStats[key]
				if !exists {
					stats = &FieldStats{
						uniqueValues:       make(map[string]bool, sampleSize),
						typeCounts:         make(map[string]int),
						numericAllIntegral: true,
					}
					fieldStats[key] = stats
				}
				mu.Unlock()

				valStr := fastString(value)
				stats.mu.Lock()
				if value != nil {
					if stats.uniqueValues[valStr] {
						stats.duplicateFound = true
					} else {
						stats.uniqueValues[valStr] = true
					}
				}
				stats.mu.Unlock()

				if value == nil {
					stats.mu.Lock()
					stats.nullable = true
					stats.mu.Unlock()
					continue
				}
				stats.mu.Lock()
				stats.countNonNull++
				stats.mu.Unlock()
				switch v := value.(type) {
				case json.RawMessage:
					stats.mu.Lock()
					stats.typeCounts["json"]++
					stats.mu.Unlock()
				case []byte:
					stats.mu.Lock()
					stats.typeCounts["bytes"]++
					stats.mu.Unlock()
				case int, int8, int16, int32, int64:
					stats.mu.Lock()
					stats.typeCounts["int"]++
					stats.hasNumeric = true
					stats.mu.Unlock()
					var num float64
					switch t := v.(type) {
					case int:
						num = float64(t)
					case int8:
						num = float64(t)
					case int16:
						num = float64(t)
					case int32:
						num = float64(t)
					case int64:
						num = float64(t)
					}
					updateNumericRange(stats, num)
				case uint, uint8, uint16, uint32, uint64:
					stats.mu.Lock()
					stats.typeCounts["int"]++
					stats.hasNumeric = true
					stats.mu.Unlock()
					var num float64
					switch t := v.(type) {
					case uint:
						num = float64(t)
					case uint8:
						num = float64(t)
					case uint16:
						num = float64(t)
					case uint32:
						num = float64(t)
					case uint64:
						num = float64(t)
					}
					updateNumericRange(stats, num)
				case float32:
					f := float64(v)
					stats.mu.Lock()
					stats.hasNumeric = true
					if f != math.Trunc(f) {
						stats.numericAllIntegral = false
						stats.typeCounts["float"]++
					} else {
						stats.typeCounts["int"]++
					}
					stats.mu.Unlock()
					updateNumericRange(stats, f)
				case float64:
					f := v
					stats.mu.Lock()
					stats.hasNumeric = true
					if f != math.Trunc(f) {
						stats.numericAllIntegral = false
						stats.typeCounts["float"]++
					} else {
						stats.typeCounts["int"]++
					}
					stats.mu.Unlock()
					updateNumericRange(stats, f)
				case json.Number:
					stats.mu.Lock()
					stats.typeCounts["json.Number"]++
					stats.hasNumeric = true
					stats.mu.Unlock()
					if i, err := v.Int64(); err == nil {
						updateNumericRange(stats, float64(i))
					} else if f, err := v.Float64(); err == nil {
						stats.mu.Lock()
						if f != math.Trunc(f) {
							stats.numericAllIntegral = false
							stats.typeCounts["float"]++
						} else {
							stats.typeCounts["int"]++
						}
						stats.mu.Unlock()
						updateNumericRange(stats, f)
					} else {
						warnf("Failed to parse json.Number: %v", v)
					}
				case bool:
					stats.mu.Lock()
					stats.typeCounts["bool"]++
					stats.mu.Unlock()
				case time.Time:
					stats.mu.Lock()
					stats.typeCounts["time"]++
					stats.mu.Unlock()
				case complex64, complex128:
					stats.mu.Lock()
					stats.typeCounts["complex"]++
					stats.mu.Unlock()
				case string:
					stats.mu.Lock()
					stats.typeCounts["string"]++
					stats.totalStringCount++
					if len(v) > stats.maxStringLength {
						stats.maxStringLength = len(v)
					}
					stats.mu.Unlock()
					if isStringTime(v) {
						stats.mu.Lock()
						stats.stringAsTimeCount++
						stats.mu.Unlock()
					}
					if isStringDate(v) {
						stats.mu.Lock()
						stats.stringAsDateCount++
						stats.mu.Unlock()
					}
					if isStringComplex(v) {
						stats.mu.Lock()
						stats.stringAsComplexCount++
						stats.mu.Unlock()
					}
					if ok, _ := isStringBool(v); ok {
						stats.mu.Lock()
						stats.stringAsBoolCount++
						stats.mu.Unlock()
					}
					if isJSONString(v) {
						stats.mu.Lock()
						stats.stringAsJsonCount++
						stats.mu.Unlock()
					}
					if isStringUUID(v) {
						stats.mu.Lock()
						stats.stringAsUUIDCount++
						stats.mu.Unlock()
					}
					if i, err := strconv.ParseInt(v, 10, 64); err == nil {
						stats.mu.Lock()
						stats.stringAsIntCount++
						stats.hasNumeric = true
						stats.mu.Unlock()
						updateNumericRange(stats, float64(i))
					} else if f, err := strconv.ParseFloat(v, 64); err == nil {
						stats.mu.Lock()
						if f != math.Trunc(f) {
							stats.stringAsFloatCount++
							stats.numericAllIntegral = false
						} else {
							stats.stringAsIntCount++
						}
						stats.hasNumeric = true
						stats.mu.Unlock()
						updateNumericRange(stats, f)
					}
				default:
					rv := reflect.ValueOf(v)
					switch rv.Kind() {
					case reflect.Slice, reflect.Array:
						stats.mu.Lock()
						stats.typeCounts["slice"]++
						stats.mu.Unlock()
					case reflect.Map:
						stats.mu.Lock()
						stats.typeCounts["map"]++
						stats.mu.Unlock()
					case reflect.Struct:
						stats.mu.Lock()
						stats.typeCounts["struct"]++
						stats.mu.Unlock()
					default:
						stats.mu.Lock()
						stats.typeCounts[reflect.TypeOf(v).String()]++
						stats.mu.Unlock()
					}
				}
			}
		}
	}

	// Start worker goroutines.
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	// Feed the rows into the channel.
	for i := 0; i < sampleSize; i++ {
		rowsCh <- data[i]
	}
	close(rowsCh)
	wg.Wait()

	// Now decide on the final type for each field.
	for field, stats := range fieldStats {
		finalType := "unknown"
		total := stats.countNonNull
		counts := stats.typeCounts
		numericCount := counts["int"] + counts["float"] + stats.stringAsIntCount + stats.stringAsFloatCount + counts["json.Number"]
		if total > 0 && (counts["time"]+stats.stringAsTimeCount == total) {
			finalType = "time"
		} else if total > 0 && (stats.stringAsDateCount == total) {
			finalType = "date"
		} else if total > 0 && (stats.stringAsUUIDCount == total) {
			finalType = "uuid"
		} else if total > 0 && (counts["complex"]+stats.stringAsComplexCount == total) {
			finalType = "complex"
		} else if total > 0 && (counts["bool"]+stats.stringAsBoolCount == total) {
			finalType = "bool"
		} else if total > 0 && numericCount == total {
			if stats.numericAllIntegral && stats.stringAsFloatCount == 0 {
				if stats.numericInitialized {
					if stats.minNumeric >= 0 && stats.maxNumeric <= float64(math.MaxUint32) {
						finalType = "uint32"
					} else if stats.minNumeric >= math.MinInt32 && stats.maxNumeric <= math.MaxInt32 {
						finalType = "int32"
					} else {
						finalType = "int64"
					}
				} else {
					finalType = "int64"
				}
			} else {
				finalType = "float64"
			}
		} else if cnt, ok := counts["slice"]; ok && cnt == total {
			finalType = "slice"
		} else if cnt, ok := counts["map"]; ok && cnt == total {
			finalType = "map"
		} else if cnt, ok := counts["struct"]; ok && cnt == total {
			finalType = "struct"
		} else if cnt, ok := counts["string"]; ok && cnt == total {
			finalType = "string"
		} else if cnt, ok := counts["json"]; ok && cnt == total {
			finalType = "json"
		} else if cnt, ok := counts["bytes"]; ok && cnt == total {
			finalType = "bytes"
		} else {
			bestType := ""
			maxCount := 0
			for t, count := range counts {
				if count > maxCount {
					bestType = t
					maxCount = count
				}
			}
			finalType = bestType
		}
		updateSchema(schema, field, finalType, stats)
	}
	return schema
}

// SetPrimaryKey marks the given keys as primary keys.
func SetPrimaryKey(schema map[string]FieldSchema, keys ...string) map[string]FieldSchema {
	if len(keys) == 0 {
		return schema
	}
	for field, s := range schema {
		if slices.Contains(keys, field) {
			s.IsPrimaryKey = true
		} else {
			s.IsPrimaryKey = false
		}
		schema[field] = s
	}
	return schema
}

// SetNullable sets fields as nullable.
func SetNullable(schema map[string]FieldSchema, keys ...string) map[string]FieldSchema {
	if len(keys) == 0 {
		return schema
	}
	for field, s := range schema {
		if slices.Contains(keys, field) {
			s.IsNullable = true
		}
		schema[field] = s
	}
	return schema
}

// MapDataTypeToDBType maps the detected type to a database-specific type.
func MapDataTypeToDBType(field FieldSchema, driver string) string {
	var mapping map[string]string
	switch driver {
	case "mysql":
		mapping = mysqlDataTypes
	case "postgres":
		mapping = postgresDataTypes
	case "sqlite":
		mapping = sqliteDataTypes
	default:
		return "TEXT"
	}
	if field.DataType == "string" {
		if driver == "mysql" && field.MaxStringLength > 255 {
			return "LONGTEXT"
		}
		if driver == "postgres" && field.MaxStringLength > 255 {
			return "TEXT"
		}
	}
	if dbType, ok := mapping[field.DataType]; ok {
		return dbType
	}
	return "TEXT"
}

func CheckJSON(b []byte) (isArray bool, err error) {
	// Check if the bytes represent valid JSON syntax.
	if !json.IsValid(string(b)) {
		return false, errors.New("invalid JSON")
	}

	// Find the first non-whitespace byte.
	for _, ch := range b {
		switch ch {
		case ' ', '\t', '\n', '\r':
			continue
		case '[':
			// Starts with '[' => a JSON array.
			return true, nil
		case '{':
			// Starts with '{' => a JSON object.
			return false, nil
		default:
			// If the first non-whitespace character is not '{' or '[',
			// it might be a JSON literal (string, number, boolean, or null).
			return false, nil
		}
	}
	return false, errors.New("empty JSON input")
}
