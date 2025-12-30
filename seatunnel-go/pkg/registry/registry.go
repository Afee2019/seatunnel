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

package registry

import (
	"fmt"
	"strings"
	"sync"

	"github.com/apache/seatunnel-go/pkg/api"
	"github.com/apache/seatunnel-go/pkg/connectors/console"
	"github.com/apache/seatunnel-go/pkg/connectors/email"
	"github.com/apache/seatunnel-go/pkg/connectors/fake"
	"github.com/apache/seatunnel-go/pkg/connectors/http"
	"github.com/apache/seatunnel-go/pkg/connectors/redis"
)

// ConnectorRegistry manages source and sink factories
type ConnectorRegistry struct {
	sourceFactories map[string]api.TableSourceFactory
	sinkFactories   map[string]api.TableSinkFactory
	mu              sync.RWMutex
}

// Global registry instance
var (
	globalRegistry *ConnectorRegistry
	once           sync.Once
)

// GetRegistry returns the global registry instance
func GetRegistry() *ConnectorRegistry {
	once.Do(func() {
		globalRegistry = NewConnectorRegistry()
		globalRegistry.RegisterBuiltinConnectors()
	})
	return globalRegistry
}

// NewConnectorRegistry creates a new connector registry
func NewConnectorRegistry() *ConnectorRegistry {
	return &ConnectorRegistry{
		sourceFactories: make(map[string]api.TableSourceFactory),
		sinkFactories:   make(map[string]api.TableSinkFactory),
	}
}

// RegisterBuiltinConnectors registers all built-in connectors
func (r *ConnectorRegistry) RegisterBuiltinConnectors() {
	// Register source factories
	r.RegisterSourceFactory(fake.NewFakeSourceFactory())
	r.RegisterSourceFactory(redis.NewRedisSourceFactory())
	r.RegisterSourceFactory(http.NewHTTPSourceFactory())

	// Register sink factories
	r.RegisterSinkFactory(console.NewConsoleSinkFactory())
	r.RegisterSinkFactory(email.NewEmailSinkFactory())
	r.RegisterSinkFactory(redis.NewRedisSinkFactory())
	r.RegisterSinkFactory(http.NewHTTPSinkFactory())
}

// RegisterSourceFactory registers a source factory
func (r *ConnectorRegistry) RegisterSourceFactory(factory api.TableSourceFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := strings.ToLower(factory.FactoryIdentifier())
	r.sourceFactories[name] = factory
}

// RegisterSinkFactory registers a sink factory
func (r *ConnectorRegistry) RegisterSinkFactory(factory api.TableSinkFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := strings.ToLower(factory.FactoryIdentifier())
	r.sinkFactories[name] = factory
}

// GetSourceFactory returns a source factory by name
func (r *ConnectorRegistry) GetSourceFactory(name string) (api.TableSourceFactory, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.sourceFactories[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("source factory not found: %s", name)
	}
	return factory, nil
}

// GetSinkFactory returns a sink factory by name
func (r *ConnectorRegistry) GetSinkFactory(name string) (api.TableSinkFactory, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.sinkFactories[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("sink factory not found: %s", name)
	}
	return factory, nil
}

// ListSourceFactories returns all registered source factory names
func (r *ConnectorRegistry) ListSourceFactories() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.sourceFactories))
	for name := range r.sourceFactories {
		names = append(names, name)
	}
	return names
}

// ListSinkFactories returns all registered sink factory names
func (r *ConnectorRegistry) ListSinkFactories() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.sinkFactories))
	for name := range r.sinkFactories {
		names = append(names, name)
	}
	return names
}

// CreateSource creates a source from configuration
func (r *ConnectorRegistry) CreateSource(name string, config *api.Config, catalogTable *api.CatalogTable) (api.Source, error) {
	factory, err := r.GetSourceFactory(name)
	if err != nil {
		return nil, err
	}

	ctx := &sourceFactoryContext{
		config:       config,
		catalogTable: catalogTable,
	}

	return factory.CreateSource(ctx)
}

// CreateSink creates a sink from configuration
func (r *ConnectorRegistry) CreateSink(name string, config *api.Config, catalogTable *api.CatalogTable) (api.Sink, error) {
	factory, err := r.GetSinkFactory(name)
	if err != nil {
		return nil, err
	}

	ctx := &sinkFactoryContext{
		config:       config,
		catalogTable: catalogTable,
	}

	return factory.CreateSink(ctx)
}

// sourceFactoryContext implements TableSourceFactoryContext
type sourceFactoryContext struct {
	config       *api.Config
	catalogTable *api.CatalogTable
}

func (c *sourceFactoryContext) GetOptions() *api.Config {
	return c.config
}

func (c *sourceFactoryContext) GetCatalogTable() *api.CatalogTable {
	return c.catalogTable
}

func (c *sourceFactoryContext) GetClassLoader() interface{} {
	return nil
}

// sinkFactoryContext implements TableSinkFactoryContext
type sinkFactoryContext struct {
	config       *api.Config
	catalogTable *api.CatalogTable
}

func (c *sinkFactoryContext) GetOptions() *api.Config {
	return c.config
}

func (c *sinkFactoryContext) GetCatalogTable() *api.CatalogTable {
	return c.catalogTable
}
