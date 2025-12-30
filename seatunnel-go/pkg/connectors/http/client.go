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

package http

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// HTTPClient is a wrapper around http.Client with retry support
type HTTPClient struct {
	config     *HTTPConfig
	httpClient *http.Client
}

// NewHTTPClient creates a new HTTP client
func NewHTTPClient(config *HTTPConfig) (*HTTPClient, error) {
	// Build TLS config
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.SkipVerify,
	}

	if config.EnableTLS {
		if config.CAFile != "" {
			caCert, err := os.ReadFile(config.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		if config.CertFile != "" && config.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	// Build transport
	transport := &http.Transport{
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}

	// Set proxy if configured
	if config.ProxyURL != "" {
		proxyURL, err := url.Parse(config.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	// Create HTTP client
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   config.ReadTimeout,
	}

	return &HTTPClient{
		config:     config,
		httpClient: httpClient,
	}, nil
}

// Request represents an HTTP request
type Request struct {
	URL     string
	Method  HTTPMethod
	Headers map[string]string
	Params  map[string]string
	Body    []byte
}

// Response represents an HTTP response
type Response struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

// Do executes an HTTP request with retry
func (c *HTTPClient) Do(req *Request) (*Response, error) {
	var lastErr error

	for attempt := 0; attempt <= c.config.RetryCount; attempt++ {
		resp, err := c.doOnce(req)
		if err == nil {
			return resp, nil
		}

		lastErr = err
		if attempt < c.config.RetryCount {
			time.Sleep(c.config.RetryInterval)
		}
	}

	return nil, fmt.Errorf("request failed after %d retries: %w", c.config.RetryCount+1, lastErr)
}

// doOnce executes a single HTTP request
func (c *HTTPClient) doOnce(req *Request) (*Response, error) {
	// Build URL with query params
	requestURL := req.URL
	if len(req.Params) > 0 {
		parsedURL, err := url.Parse(requestURL)
		if err != nil {
			return nil, fmt.Errorf("invalid URL: %w", err)
		}
		query := parsedURL.Query()
		for k, v := range req.Params {
			query.Set(k, v)
		}
		parsedURL.RawQuery = query.Encode()
		requestURL = parsedURL.String()
	}

	// Create request
	var body io.Reader
	if len(req.Body) > 0 {
		body = bytes.NewReader(req.Body)
	}

	httpReq, err := http.NewRequest(string(req.Method), requestURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set default headers from config
	for k, v := range c.config.Headers {
		httpReq.Header.Set(k, v)
	}

	// Set request-specific headers
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	// Set content type
	if len(req.Body) > 0 && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", string(c.config.ContentType))
	}

	// Set authentication
	c.setAuth(httpReq)

	// Execute request
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return &Response{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header,
		Body:       respBody,
	}, nil
}

// setAuth sets authentication headers
func (c *HTTPClient) setAuth(req *http.Request) {
	switch c.config.AuthType {
	case "basic":
		auth := base64.StdEncoding.EncodeToString(
			[]byte(c.config.Username + ":" + c.config.Password))
		req.Header.Set("Authorization", "Basic "+auth)

	case "bearer":
		req.Header.Set("Authorization", "Bearer "+c.config.BearerToken)

	case "api_key":
		header := c.config.APIKeyHeader
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, c.config.APIKey)
	}
}

// Get performs a GET request
func (c *HTTPClient) Get(url string, params map[string]string) (*Response, error) {
	return c.Do(&Request{
		URL:    url,
		Method: MethodGET,
		Params: params,
	})
}

// Post performs a POST request
func (c *HTTPClient) Post(url string, body []byte) (*Response, error) {
	return c.Do(&Request{
		URL:    url,
		Method: MethodPOST,
		Body:   body,
	})
}

// JSONPath extracts a value from JSON using a simple dot-notation path
func JSONPath(data []byte, path string) (interface{}, error) {
	if path == "" || path == "$" {
		var result interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}
		return result, nil
	}

	// Remove leading $. if present
	path = strings.TrimPrefix(path, "$.")
	path = strings.TrimPrefix(path, "$")

	var current interface{}
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, err
	}

	parts := strings.Split(path, ".")
	for _, part := range parts {
		if current == nil {
			return nil, nil
		}

		switch v := current.(type) {
		case map[string]interface{}:
			current = v[part]
		case []interface{}:
			// Try to parse part as index
			if part == "*" {
				return v, nil
			}
			return nil, fmt.Errorf("cannot index array with '%s'", part)
		default:
			return nil, fmt.Errorf("cannot traverse '%s' in non-object/array", part)
		}
	}

	return current, nil
}

// JSONPathString extracts a string value
func JSONPathString(data []byte, path string) (string, error) {
	val, err := JSONPath(data, path)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	if s, ok := val.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", val), nil
}

// JSONPathInt extracts an integer value
func JSONPathInt(data []byte, path string) (int64, error) {
	val, err := JSONPath(data, path)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	default:
		return 0, fmt.Errorf("value is not a number: %v", val)
	}
}

// JSONPathArray extracts an array value
func JSONPathArray(data []byte, path string) ([]interface{}, error) {
	val, err := JSONPath(data, path)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if arr, ok := val.([]interface{}); ok {
		return arr, nil
	}
	return nil, fmt.Errorf("value is not an array: %v", val)
}
