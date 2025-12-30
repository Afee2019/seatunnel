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

package api

import (
	"fmt"
	"time"
)

// SqlType represents the SQL type enumeration
type SqlType int

const (
	// Basic types
	NULL SqlType = iota
	BOOLEAN
	TINYINT
	SMALLINT
	INT
	BIGINT
	FLOAT
	DOUBLE
	DECIMAL
	STRING
	BYTES
	DATE
	TIME
	TIMESTAMP

	// Complex types
	ARRAY
	MAP
	ROW

	// Vector types (for ML/AI workloads)
	BINARY_VECTOR
	FLOAT_VECTOR
	FLOAT16_VECTOR
	BFLOAT16_VECTOR
	SPARSE_FLOAT_VECTOR
)

// String returns the string representation of SqlType
func (t SqlType) String() string {
	names := []string{
		"NULL", "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT",
		"FLOAT", "DOUBLE", "DECIMAL", "STRING", "BYTES",
		"DATE", "TIME", "TIMESTAMP",
		"ARRAY", "MAP", "ROW",
		"BINARY_VECTOR", "FLOAT_VECTOR", "FLOAT16_VECTOR",
		"BFLOAT16_VECTOR", "SPARSE_FLOAT_VECTOR",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return fmt.Sprintf("UNKNOWN(%d)", t)
}

// SeaTunnelDataType represents a data type in SeaTunnel
type SeaTunnelDataType struct {
	SqlType     SqlType
	Precision   int
	Scale       int
	ElementType *SeaTunnelDataType            // For ARRAY
	KeyType     *SeaTunnelDataType            // For MAP
	ValueType   *SeaTunnelDataType            // For MAP
	RowType     *SeaTunnelRowType             // For ROW
	Dimension   int                           // For VECTOR types
}

// NewBasicType creates a basic data type
func NewBasicType(sqlType SqlType) *SeaTunnelDataType {
	return &SeaTunnelDataType{SqlType: sqlType}
}

// NewDecimalType creates a decimal type with precision and scale
func NewDecimalType(precision, scale int) *SeaTunnelDataType {
	return &SeaTunnelDataType{
		SqlType:   DECIMAL,
		Precision: precision,
		Scale:     scale,
	}
}

// NewArrayType creates an array type
func NewArrayType(elementType *SeaTunnelDataType) *SeaTunnelDataType {
	return &SeaTunnelDataType{
		SqlType:     ARRAY,
		ElementType: elementType,
	}
}

// NewMapType creates a map type
func NewMapType(keyType, valueType *SeaTunnelDataType) *SeaTunnelDataType {
	return &SeaTunnelDataType{
		SqlType:   MAP,
		KeyType:   keyType,
		ValueType: valueType,
	}
}

// NewRowType creates a nested row type
func NewRowType(rowType *SeaTunnelRowType) *SeaTunnelDataType {
	return &SeaTunnelDataType{
		SqlType: ROW,
		RowType: rowType,
	}
}

// NewVectorType creates a vector type
func NewVectorType(sqlType SqlType, dimension int) *SeaTunnelDataType {
	return &SeaTunnelDataType{
		SqlType:   sqlType,
		Dimension: dimension,
	}
}

// IsBasicType returns true if this is a basic type
func (t *SeaTunnelDataType) IsBasicType() bool {
	return t.SqlType >= NULL && t.SqlType <= TIMESTAMP
}

// IsComplexType returns true if this is a complex type
func (t *SeaTunnelDataType) IsComplexType() bool {
	return t.SqlType >= ARRAY && t.SqlType <= ROW
}

// IsVectorType returns true if this is a vector type
func (t *SeaTunnelDataType) IsVectorType() bool {
	return t.SqlType >= BINARY_VECTOR && t.SqlType <= SPARSE_FLOAT_VECTOR
}

// SeaTunnelFieldType represents a field in a row type
type SeaTunnelFieldType struct {
	Name     string
	Type     *SeaTunnelDataType
	Nullable bool
	Comment  string
}

// NewField creates a new field type
func NewField(name string, dataType *SeaTunnelDataType) *SeaTunnelFieldType {
	return &SeaTunnelFieldType{
		Name:     name,
		Type:     dataType,
		Nullable: true,
	}
}

// SeaTunnelRowType represents the schema of a row
type SeaTunnelRowType struct {
	Fields []*SeaTunnelFieldType
}

// NewRowSchema creates a new row type with the given fields
func NewRowSchema(fields ...*SeaTunnelFieldType) *SeaTunnelRowType {
	return &SeaTunnelRowType{Fields: fields}
}

// GetFieldCount returns the number of fields
func (r *SeaTunnelRowType) GetFieldCount() int {
	return len(r.Fields)
}

// GetFieldNames returns all field names
func (r *SeaTunnelRowType) GetFieldNames() []string {
	names := make([]string, len(r.Fields))
	for i, f := range r.Fields {
		names[i] = f.Name
	}
	return names
}

// GetFieldTypes returns all field types
func (r *SeaTunnelRowType) GetFieldTypes() []*SeaTunnelDataType {
	types := make([]*SeaTunnelDataType, len(r.Fields))
	for i, f := range r.Fields {
		types[i] = f.Type
	}
	return types
}

// GetFieldIndex returns the index of a field by name, -1 if not found
func (r *SeaTunnelRowType) GetFieldIndex(name string) int {
	for i, f := range r.Fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// Boundedness represents whether a source is bounded or unbounded
type Boundedness int

const (
	// Bounded source (batch mode)
	Bounded Boundedness = iota
	// Unbounded source (streaming mode)
	Unbounded
)

// RowKind represents the kind of change a row represents
type RowKind int

const (
	// Insert represents an insertion
	Insert RowKind = iota
	// UpdateBefore represents the before image of an update
	UpdateBefore
	// UpdateAfter represents the after image of an update
	UpdateAfter
	// Delete represents a deletion
	Delete
)

// String returns the string representation of RowKind
func (k RowKind) String() string {
	switch k {
	case Insert:
		return "+I"
	case UpdateBefore:
		return "-U"
	case UpdateAfter:
		return "+U"
	case Delete:
		return "-D"
	default:
		return "?"
	}
}

// Decimal represents a decimal number with precision and scale
type Decimal struct {
	Value     string
	Precision int
	Scale     int
}

// LocalDate represents a date without time zone
type LocalDate struct {
	Year  int
	Month int
	Day   int
}

// ToTime converts LocalDate to time.Time
func (d LocalDate) ToTime() time.Time {
	return time.Date(d.Year, time.Month(d.Month), d.Day, 0, 0, 0, 0, time.UTC)
}

// LocalTime represents a time without date and time zone
type LocalTime struct {
	Hour       int
	Minute     int
	Second     int
	Nanosecond int
}

// LocalDateTime represents a date-time without time zone
type LocalDateTime struct {
	Date LocalDate
	Time LocalTime
}

// ToTime converts LocalDateTime to time.Time
func (dt LocalDateTime) ToTime() time.Time {
	return time.Date(
		dt.Date.Year, time.Month(dt.Date.Month), dt.Date.Day,
		dt.Time.Hour, dt.Time.Minute, dt.Time.Second, dt.Time.Nanosecond,
		time.UTC,
	)
}
