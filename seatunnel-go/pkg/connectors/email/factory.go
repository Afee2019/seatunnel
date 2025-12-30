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

package email

import (
	"fmt"

	"github.com/apache/seatunnel-go/pkg/api"
)

// EmailSinkFactory creates EmailSink instances
type EmailSinkFactory struct{}

// NewEmailSinkFactory creates a new factory
func NewEmailSinkFactory() *EmailSinkFactory {
	return &EmailSinkFactory{}
}

// FactoryIdentifier returns the identifier
func (f *EmailSinkFactory) FactoryIdentifier() string {
	return PluginName
}

// OptionRules returns the configuration options
func (f *EmailSinkFactory) OptionRules() []api.OptionRule {
	return []api.OptionRule{
		{
			Name:        "email_host",
			Type:        api.OptionTypeString,
			Required:    true,
			Description: "SMTP server hostname",
		},
		{
			Name:         "email_transport_protocol",
			Type:         api.OptionTypeString,
			Required:     false,
			DefaultValue: "smtp",
			Description:  "Email transport protocol (smtp)",
		},
		{
			Name:         "email_smtp_auth",
			Type:         api.OptionTypeBoolean,
			Required:     false,
			DefaultValue: true,
			Description:  "Whether SMTP authentication is required",
		},
		{
			Name:        "email_authorization_code",
			Type:        api.OptionTypeString,
			Required:    false,
			Description: "SMTP authorization code/password",
		},
		{
			Name:        "email_from_address",
			Type:        api.OptionTypeString,
			Required:    true,
			Description: "Sender email address",
		},
		{
			Name:        "email_to_address",
			Type:        api.OptionTypeString,
			Required:    true,
			Description: "Recipient email addresses (comma-separated)",
		},
		{
			Name:        "email_cc_address",
			Type:        api.OptionTypeString,
			Required:    false,
			Description: "CC email addresses (comma-separated)",
		},
		{
			Name:        "email_bcc_address",
			Type:        api.OptionTypeString,
			Required:    false,
			Description: "BCC email addresses (comma-separated)",
		},
		{
			Name:         "email_message_headline",
			Type:         api.OptionTypeString,
			Required:     false,
			DefaultValue: "SeaTunnel Data Report",
			Description:  "Email subject",
		},
		{
			Name:         "email_message_content",
			Type:         api.OptionTypeString,
			Required:     false,
			Description:  "Email body template",
		},
		{
			Name:         "email_content_type",
			Type:         api.OptionTypeString,
			Required:     false,
			DefaultValue: "text",
			Description:  "Content type: text or html",
		},
		{
			Name:         "email_smtp_port",
			Type:         api.OptionTypeInt,
			Required:     false,
			DefaultValue: 587,
			Description:  "SMTP server port",
		},
		{
			Name:         "email_smtp_ssl",
			Type:         api.OptionTypeBoolean,
			Required:     false,
			DefaultValue: false,
			Description:  "Use SSL connection",
		},
		{
			Name:         "email_smtp_tls",
			Type:         api.OptionTypeBoolean,
			Required:     false,
			DefaultValue: true,
			Description:  "Use TLS connection",
		},
	}
}

// CreateSink creates an email sink
func (f *EmailSinkFactory) CreateSink(ctx api.TableSinkFactoryContext) (api.Sink, error) {
	config := ctx.GetOptions()

	emailConfig := &EmailConfig{
		SMTPHost:     config.GetString("email_host"),
		SMTPPort:     config.GetIntDefault("email_smtp_port", 587),
		SMTPAuth:     config.GetBoolDefault("email_smtp_auth", true),
		SMTPUser:     config.GetString("email_from_address"),
		SMTPPassword: config.GetString("email_authorization_code"),
		SMTPSSL:      config.GetBoolDefault("email_smtp_ssl", false),
		SMTPTLS:      config.GetBoolDefault("email_smtp_tls", true),
		FromEmail:    config.GetString("email_from_address"),
		Subject:      config.GetStringDefault("email_message_headline", "SeaTunnel Data Report"),
		ContentType:  config.GetStringDefault("email_content_type", "text"),
		BodyTemplate: config.GetString("email_message_content"),
	}

	// Parse email lists
	toAddrs := config.GetString("email_to_address")
	if toAddrs == "" {
		return nil, fmt.Errorf("email_to_address is required")
	}
	emailConfig.ToEmails = splitEmails(toAddrs)

	if ccAddrs := config.GetString("email_cc_address"); ccAddrs != "" {
		emailConfig.CcEmails = splitEmails(ccAddrs)
	}

	if bccAddrs := config.GetString("email_bcc_address"); bccAddrs != "" {
		emailConfig.BccEmails = splitEmails(bccAddrs)
	}

	sink := NewEmailSink(emailConfig)
	if ctx.GetCatalogTable() != nil {
		sink.SetTypeInfo(ctx.GetCatalogTable().TableSchema)
	}

	return sink, nil
}

// splitEmails splits comma-separated email addresses
func splitEmails(s string) []string {
	var result []string
	for _, email := range splitAndTrim(s, ",") {
		if email != "" {
			result = append(result, email)
		}
	}
	return result
}

// splitAndTrim splits a string and trims each part
func splitAndTrim(s, sep string) []string {
	parts := make([]string, 0)
	for _, part := range splitString(s, sep) {
		trimmed := trimSpace(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

// splitString is a simple string split
func splitString(s, sep string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
		}
	}
	result = append(result, s[start:])
	return result
}

// trimSpace trims whitespace
func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}
