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
	"reflect"
	"strings"
	"time"
)

// SeaTunnelRow represents a row of data in SeaTunnel
type SeaTunnelRow struct {
	tableID string
	kind    RowKind
	fields  []interface{}
}

// NewSeaTunnelRow creates a new row with the given fields
func NewSeaTunnelRow(fields []interface{}) *SeaTunnelRow {
	return &SeaTunnelRow{
		kind:   Insert,
		fields: fields,
	}
}

// NewSeaTunnelRowWithKind creates a new row with kind and fields
func NewSeaTunnelRowWithKind(kind RowKind, fields []interface{}) *SeaTunnelRow {
	return &SeaTunnelRow{
		kind:   kind,
		fields: fields,
	}
}

// GetTableID returns the table identifier
func (r *SeaTunnelRow) GetTableID() string {
	return r.tableID
}

// SetTableID sets the table identifier
func (r *SeaTunnelRow) SetTableID(tableID string) {
	r.tableID = tableID
}

// GetKind returns the row kind
func (r *SeaTunnelRow) GetKind() RowKind {
	return r.kind
}

// SetKind sets the row kind
func (r *SeaTunnelRow) SetKind(kind RowKind) {
	r.kind = kind
}

// GetArity returns the number of fields
func (r *SeaTunnelRow) GetArity() int {
	return len(r.fields)
}

// GetField returns the field at the given index
func (r *SeaTunnelRow) GetField(index int) interface{} {
	if index < 0 || index >= len(r.fields) {
		return nil
	}
	return r.fields[index]
}

// SetField sets the field at the given index
func (r *SeaTunnelRow) SetField(index int, value interface{}) {
	if index >= 0 && index < len(r.fields) {
		r.fields[index] = value
	}
}

// GetFields returns all fields
func (r *SeaTunnelRow) GetFields() []interface{} {
	return r.fields
}

// IsNullAt returns true if the field at the given index is null
func (r *SeaTunnelRow) IsNullAt(index int) bool {
	if index < 0 || index >= len(r.fields) {
		return true
	}
	return r.fields[index] == nil
}

// Copy creates a deep copy of the row
func (r *SeaTunnelRow) Copy() *SeaTunnelRow {
	newFields := make([]interface{}, len(r.fields))
	copy(newFields, r.fields)
	return &SeaTunnelRow{
		tableID: r.tableID,
		kind:    r.kind,
		fields:  newFields,
	}
}

// String returns a string representation of the row
func (r *SeaTunnelRow) String() string {
	var sb strings.Builder
	sb.WriteString(r.kind.String())
	sb.WriteString("(")
	for i, field := range r.fields {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(formatValue(field))
	}
	sb.WriteString(")")
	return sb.String()
}

// formatValue formats a value for string representation
func formatValue(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case bool:
		return fmt.Sprintf("%t", v)
	case int8, int16, int32, int64, int:
		return fmt.Sprintf("%d", v)
	case uint8, uint16, uint32, uint64, uint:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case string:
		return v
	case []byte:
		return fmt.Sprintf("[%d bytes]", len(v))
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case LocalDate:
		return fmt.Sprintf("%04d-%02d-%02d", v.Year, v.Month, v.Day)
	case LocalTime:
		return fmt.Sprintf("%02d:%02d:%02d.%09d", v.Hour, v.Minute, v.Second, v.Nanosecond)
	case LocalDateTime:
		return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%09d",
			v.Date.Year, v.Date.Month, v.Date.Day,
			v.Time.Hour, v.Time.Minute, v.Time.Second, v.Time.Nanosecond)
	case Decimal:
		return v.Value
	case *SeaTunnelRow:
		return v.String()
	case []interface{}:
		var items []string
		for _, item := range v {
			items = append(items, formatValue(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[interface{}]interface{}:
		var items []string
		for k, val := range v {
			items = append(items, fmt.Sprintf("%s=%s", formatValue(k), formatValue(val)))
		}
		return "{" + strings.Join(items, ", ") + "}"
	default:
		rv := reflect.ValueOf(value)
		if rv.Kind() == reflect.Slice {
			var items []string
			for i := 0; i < rv.Len(); i++ {
				items = append(items, formatValue(rv.Index(i).Interface()))
			}
			return "[" + strings.Join(items, ", ") + "]"
		}
		return fmt.Sprintf("%v", v)
	}
}

// FormatField formats a field value according to its type
func FormatField(dataType *SeaTunnelDataType, value interface{}) string {
	if value == nil {
		return "null"
	}
	return formatValue(value)
}

// RowBuilder helps build SeaTunnelRow instances
type RowBuilder struct {
	rowType *SeaTunnelRowType
	fields  []interface{}
	kind    RowKind
	tableID string
}

// NewRowBuilder creates a new row builder for the given schema
func NewRowBuilder(rowType *SeaTunnelRowType) *RowBuilder {
	return &RowBuilder{
		rowType: rowType,
		fields:  make([]interface{}, rowType.GetFieldCount()),
		kind:    Insert,
	}
}

// SetKind sets the row kind
func (b *RowBuilder) SetKind(kind RowKind) *RowBuilder {
	b.kind = kind
	return b
}

// SetTableID sets the table identifier
func (b *RowBuilder) SetTableID(tableID string) *RowBuilder {
	b.tableID = tableID
	return b
}

// Set sets a field value by index
func (b *RowBuilder) Set(index int, value interface{}) *RowBuilder {
	if index >= 0 && index < len(b.fields) {
		b.fields[index] = value
	}
	return b
}

// SetByName sets a field value by name
func (b *RowBuilder) SetByName(name string, value interface{}) *RowBuilder {
	index := b.rowType.GetFieldIndex(name)
	if index >= 0 {
		b.fields[index] = value
	}
	return b
}

// Build creates the SeaTunnelRow
func (b *RowBuilder) Build() *SeaTunnelRow {
	row := &SeaTunnelRow{
		tableID: b.tableID,
		kind:    b.kind,
		fields:  make([]interface{}, len(b.fields)),
	}
	copy(row.fields, b.fields)
	return row
}

// Reset resets the builder for reuse
func (b *RowBuilder) Reset() *RowBuilder {
	for i := range b.fields {
		b.fields[i] = nil
	}
	b.kind = Insert
	b.tableID = ""
	return b
}
