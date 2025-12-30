// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package fake

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/apache/seatunnel-go/pkg/api"
	"github.com/brianvoe/gofakeit/v7"
)

// FakeDataGenerator generates fake data according to schema
type FakeDataGenerator struct {
	schema  *api.SeaTunnelRowType
	faker   *gofakeit.Faker
	options *FakeOptions
	rnd     *rand.Rand
}

// FakeOptions contains generation options
type FakeOptions struct {
	// Row generation
	RowCount      int64
	SplitNum      int
	SplitReadInterval int64 // milliseconds

	// String options
	StringLength      int
	StringFakeMode    StringFakeMode

	// Numeric options
	TinyintMin   int8
	TinyintMax   int8
	SmallintMin  int16
	SmallintMax  int16
	IntMin       int32
	IntMax       int32
	BigintMin    int64
	BigintMax    int64
	FloatMin     float32
	FloatMax     float32
	DoubleMin    float64
	DoubleMax    float64

	// Date/Time options
	DateYearRange  int
	TimeHourRange  int

	// Array options
	ArraySize int

	// Map options
	MapSize int

	// Bytes options
	BytesLength int

	// Vector options
	VectorDimension int
}

// StringFakeMode defines how strings are generated
type StringFakeMode int

const (
	StringFakeModeRange StringFakeMode = iota
	StringFakeModeSentence
	StringFakeModeWord
	StringFakeModeUUID
	StringFakeModeName
	StringFakeModeEmail
	StringFakeModePhone
	StringFakeModeAddress
)

// DefaultFakeOptions returns default options
func DefaultFakeOptions() *FakeOptions {
	return &FakeOptions{
		RowCount:          10,
		SplitNum:          1,
		SplitReadInterval: 0,
		StringLength:      50,
		StringFakeMode:    StringFakeModeRange,
		TinyintMin:        -128,
		TinyintMax:        127,
		SmallintMin:       -32768,
		SmallintMax:       32767,
		IntMin:            -1000000,
		IntMax:            1000000,
		BigintMin:         -1000000000,
		BigintMax:         1000000000,
		FloatMin:          -1000000,
		FloatMax:          1000000,
		DoubleMin:         -1000000,
		DoubleMax:         1000000,
		DateYearRange:     10,
		TimeHourRange:     24,
		ArraySize:         3,
		MapSize:           3,
		BytesLength:       16,
		VectorDimension:   128,
	}
}

// NewFakeDataGenerator creates a new data generator
func NewFakeDataGenerator(schema *api.SeaTunnelRowType, options *FakeOptions) *FakeDataGenerator {
	if options == nil {
		options = DefaultFakeOptions()
	}

	seed := time.Now().UnixNano()
	return &FakeDataGenerator{
		schema:  schema,
		faker:   gofakeit.New(uint64(seed)),
		options: options,
		rnd:     rand.New(rand.NewSource(seed)),
	}
}

// Generate generates a single row
func (g *FakeDataGenerator) Generate() *api.SeaTunnelRow {
	fields := make([]interface{}, len(g.schema.Fields))
	for i, field := range g.schema.Fields {
		fields[i] = g.generateValue(field.Type)
	}
	return api.NewSeaTunnelRow(fields)
}

// generateValue generates a value for a given type
func (g *FakeDataGenerator) generateValue(dataType *api.SeaTunnelDataType) interface{} {
	switch dataType.SqlType {
	case api.NULL:
		return nil

	case api.BOOLEAN:
		return g.faker.Bool()

	case api.TINYINT:
		return g.randomInt8(g.options.TinyintMin, g.options.TinyintMax)

	case api.SMALLINT:
		return g.randomInt16(g.options.SmallintMin, g.options.SmallintMax)

	case api.INT:
		return g.randomInt32(g.options.IntMin, g.options.IntMax)

	case api.BIGINT:
		return g.randomInt64(g.options.BigintMin, g.options.BigintMax)

	case api.FLOAT:
		return g.randomFloat32(g.options.FloatMin, g.options.FloatMax)

	case api.DOUBLE:
		return g.randomFloat64(g.options.DoubleMin, g.options.DoubleMax)

	case api.DECIMAL:
		value := g.randomFloat64(-1000000, 1000000)
		return api.Decimal{
			Value:     fmt.Sprintf("%.4f", value),
			Precision: dataType.Precision,
			Scale:     dataType.Scale,
		}

	case api.STRING:
		return g.generateString()

	case api.BYTES:
		bytes := make([]byte, g.options.BytesLength)
		for i := range bytes {
			bytes[i] = byte(g.rnd.Intn(256))
		}
		return bytes

	case api.DATE:
		return g.generateLocalDate()

	case api.TIME:
		return g.generateLocalTime()

	case api.TIMESTAMP:
		return g.generateLocalDateTime()

	case api.ARRAY:
		return g.generateArray(dataType.ElementType)

	case api.MAP:
		return g.generateMap(dataType.KeyType, dataType.ValueType)

	case api.ROW:
		return g.generateRow(dataType.RowType)

	case api.BINARY_VECTOR:
		return g.generateBinaryVector(dataType.Dimension)

	case api.FLOAT_VECTOR:
		return g.generateFloatVector(dataType.Dimension)

	case api.FLOAT16_VECTOR, api.BFLOAT16_VECTOR:
		return g.generateFloatVector(dataType.Dimension)

	case api.SPARSE_FLOAT_VECTOR:
		return g.generateSparseFloatVector(dataType.Dimension)

	default:
		return nil
	}
}

// generateString generates a string based on mode
func (g *FakeDataGenerator) generateString() string {
	switch g.options.StringFakeMode {
	case StringFakeModeSentence:
		return g.faker.Sentence(5)
	case StringFakeModeWord:
		return g.faker.Word()
	case StringFakeModeUUID:
		return g.faker.UUID()
	case StringFakeModeName:
		return g.faker.Name()
	case StringFakeModeEmail:
		return g.faker.Email()
	case StringFakeModePhone:
		return g.faker.Phone()
	case StringFakeModeAddress:
		return g.faker.Address().Address
	default:
		return g.faker.LetterN(uint(g.options.StringLength))
	}
}

// generateLocalDate generates a random date
func (g *FakeDataGenerator) generateLocalDate() api.LocalDate {
	now := time.Now()
	yearOffset := g.rnd.Intn(g.options.DateYearRange*2) - g.options.DateYearRange
	year := now.Year() + yearOffset
	month := g.rnd.Intn(12) + 1
	day := g.rnd.Intn(28) + 1 // Simple approach to avoid invalid dates

	return api.LocalDate{Year: year, Month: month, Day: day}
}

// generateLocalTime generates a random time
func (g *FakeDataGenerator) generateLocalTime() api.LocalTime {
	return api.LocalTime{
		Hour:       g.rnd.Intn(g.options.TimeHourRange),
		Minute:     g.rnd.Intn(60),
		Second:     g.rnd.Intn(60),
		Nanosecond: g.rnd.Intn(1000000000),
	}
}

// generateLocalDateTime generates a random date-time
func (g *FakeDataGenerator) generateLocalDateTime() api.LocalDateTime {
	return api.LocalDateTime{
		Date: g.generateLocalDate(),
		Time: g.generateLocalTime(),
	}
}

// generateArray generates an array
func (g *FakeDataGenerator) generateArray(elementType *api.SeaTunnelDataType) []interface{} {
	size := g.options.ArraySize
	arr := make([]interface{}, size)
	for i := 0; i < size; i++ {
		arr[i] = g.generateValue(elementType)
	}
	return arr
}

// generateMap generates a map
func (g *FakeDataGenerator) generateMap(keyType, valueType *api.SeaTunnelDataType) map[interface{}]interface{} {
	size := g.options.MapSize
	m := make(map[interface{}]interface{}, size)
	for i := 0; i < size; i++ {
		key := g.generateValue(keyType)
		value := g.generateValue(valueType)
		m[key] = value
	}
	return m
}

// generateRow generates a nested row
func (g *FakeDataGenerator) generateRow(rowType *api.SeaTunnelRowType) *api.SeaTunnelRow {
	subGenerator := &FakeDataGenerator{
		schema:  rowType,
		faker:   g.faker,
		options: g.options,
		rnd:     g.rnd,
	}
	return subGenerator.Generate()
}

// generateBinaryVector generates a binary vector
func (g *FakeDataGenerator) generateBinaryVector(dimension int) []byte {
	if dimension <= 0 {
		dimension = g.options.VectorDimension
	}
	byteLen := (dimension + 7) / 8
	bytes := make([]byte, byteLen)
	for i := range bytes {
		bytes[i] = byte(g.rnd.Intn(256))
	}
	return bytes
}

// generateFloatVector generates a float vector
func (g *FakeDataGenerator) generateFloatVector(dimension int) []float32 {
	if dimension <= 0 {
		dimension = g.options.VectorDimension
	}
	vec := make([]float32, dimension)
	for i := range vec {
		vec[i] = g.rnd.Float32()*2 - 1 // -1 to 1
	}
	return vec
}

// generateSparseFloatVector generates a sparse float vector as map
func (g *FakeDataGenerator) generateSparseFloatVector(dimension int) map[int]float32 {
	if dimension <= 0 {
		dimension = g.options.VectorDimension
	}
	// Generate sparse vector with ~10% density
	sparseSize := dimension / 10
	if sparseSize < 1 {
		sparseSize = 1
	}

	vec := make(map[int]float32, sparseSize)
	for i := 0; i < sparseSize; i++ {
		idx := g.rnd.Intn(dimension)
		vec[idx] = g.rnd.Float32()*2 - 1
	}
	return vec
}

// Random number generators

func (g *FakeDataGenerator) randomInt8(min, max int8) int8 {
	if min >= max {
		return min
	}
	return int8(g.rnd.Intn(int(max-min+1))) + min
}

func (g *FakeDataGenerator) randomInt16(min, max int16) int16 {
	if min >= max {
		return min
	}
	return int16(g.rnd.Intn(int(max-min+1))) + min
}

func (g *FakeDataGenerator) randomInt32(min, max int32) int32 {
	if min >= max {
		return min
	}
	return int32(g.rnd.Int63n(int64(max-min+1))) + min
}

func (g *FakeDataGenerator) randomInt64(min, max int64) int64 {
	if min >= max {
		return min
	}
	// Avoid overflow and ensure positive range
	rangeSize := max - min
	if rangeSize <= 0 {
		return min
	}
	return g.rnd.Int63n(rangeSize+1) + min
}

func (g *FakeDataGenerator) randomFloat32(min, max float32) float32 {
	return min + g.rnd.Float32()*(max-min)
}

func (g *FakeDataGenerator) randomFloat64(min, max float64) float64 {
	return min + g.rnd.Float64()*(max-min)
}
