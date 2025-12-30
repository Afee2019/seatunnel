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
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"github.com/apache/seatunnel-go/pkg/api"
	mail "github.com/wneessen/go-mail"
	"go.uber.org/zap"
)

const (
	// PluginName is the name of the email sink plugin
	PluginName = "EmailSink"
)

// EmailConfig contains email configuration
type EmailConfig struct {
	// SMTP settings
	SMTPHost     string
	SMTPPort     int
	SMTPAuth     bool
	SMTPUser     string
	SMTPPassword string
	SMTPSSL      bool
	SMTPTLS      bool

	// Email settings
	FromEmail    string
	ToEmails     []string
	CcEmails     []string
	BccEmails    []string
	Subject      string
	ContentType  string // text or html
	BodyTemplate string
}

// EmailSink is a sink that sends data via email
type EmailSink struct {
	config   *EmailConfig
	rowType  *api.SeaTunnelRowType
}

// NewEmailSink creates a new email sink
func NewEmailSink(config *EmailConfig) *EmailSink {
	return &EmailSink{
		config: config,
	}
}

// GetPluginName returns the plugin name
func (s *EmailSink) GetPluginName() string {
	return PluginName
}

// SetTypeInfo sets the input row type
func (s *EmailSink) SetTypeInfo(rowType *api.SeaTunnelRowType) {
	s.rowType = rowType
}

// GetConsumedType returns the consumed row type
func (s *EmailSink) GetConsumedType() *api.SeaTunnelRowType {
	return s.rowType
}

// CreateWriter creates an email sink writer
func (s *EmailSink) CreateWriter(ctx api.WriterContext) (api.SinkWriter, error) {
	return NewEmailSinkWriter(s.config, s.rowType, ctx.GetIndexOfSubtask())
}

// CreateCommitter returns nil
func (s *EmailSink) CreateCommitter() api.SinkCommitter {
	return nil
}

// CreateAggregatedCommitter returns nil
func (s *EmailSink) CreateAggregatedCommitter() api.SinkAggregatedCommitter {
	return nil
}

// EmailSinkWriter writes rows to email
type EmailSinkWriter struct {
	config       *EmailConfig
	rowType      *api.SeaTunnelRowType
	subtaskIndex int
	buffer       []*api.SeaTunnelRow
	bodyTemplate *template.Template
	logger       *zap.Logger
	mu           sync.Mutex
}

// NewEmailSinkWriter creates a new email sink writer
func NewEmailSinkWriter(config *EmailConfig, rowType *api.SeaTunnelRowType, subtaskIndex int) (*EmailSinkWriter, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	var tmpl *template.Template
	if config.BodyTemplate != "" {
		tmpl, err = template.New("email").Parse(config.BodyTemplate)
		if err != nil {
			return nil, fmt.Errorf("failed to parse body template: %w", err)
		}
	}

	return &EmailSinkWriter{
		config:       config,
		rowType:      rowType,
		subtaskIndex: subtaskIndex,
		buffer:       make([]*api.SeaTunnelRow, 0),
		bodyTemplate: tmpl,
		logger:       logger,
	}, nil
}

// Write writes a row to the buffer
func (w *EmailSinkWriter) Write(row *api.SeaTunnelRow) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buffer = append(w.buffer, row.Copy())
	return nil
}

// PrepareCommit prepares for commit by sending the email
func (w *EmailSinkWriter) PrepareCommit() ([]api.CommitInfo, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buffer) == 0 {
		return nil, nil
	}

	// Build email body
	body, err := w.buildEmailBody()
	if err != nil {
		return nil, fmt.Errorf("failed to build email body: %w", err)
	}

	// Send email
	if err := w.sendEmail(body); err != nil {
		return nil, fmt.Errorf("failed to send email: %w", err)
	}

	w.logger.Info("Email sent successfully",
		zap.Int("subtaskIndex", w.subtaskIndex),
		zap.Int("rowCount", len(w.buffer)),
	)

	// Clear buffer
	w.buffer = w.buffer[:0]

	return nil, nil
}

// buildEmailBody builds the email body from buffered rows
func (w *EmailSinkWriter) buildEmailBody() (string, error) {
	if w.bodyTemplate != nil {
		// Use template
		data := map[string]interface{}{
			"Rows":    w.buffer,
			"RowType": w.rowType,
			"Count":   len(w.buffer),
		}

		var buf bytes.Buffer
		if err := w.bodyTemplate.Execute(&buf, data); err != nil {
			return "", err
		}
		return buf.String(), nil
	}

	// Default formatting
	var sb strings.Builder

	if w.config.ContentType == "html" {
		sb.WriteString("<html><body>")
		sb.WriteString("<h2>SeaTunnel Data Report</h2>")
		sb.WriteString(fmt.Sprintf("<p>Total Rows: %d</p>", len(w.buffer)))
		sb.WriteString("<table border='1' cellpadding='5' cellspacing='0'>")

		// Header row
		if w.rowType != nil {
			sb.WriteString("<tr>")
			for _, field := range w.rowType.Fields {
				sb.WriteString(fmt.Sprintf("<th>%s</th>", field.Name))
			}
			sb.WriteString("</tr>")
		}

		// Data rows
		for _, row := range w.buffer {
			sb.WriteString("<tr>")
			for i := 0; i < row.GetArity(); i++ {
				var fieldType *api.SeaTunnelDataType
				if w.rowType != nil && i < len(w.rowType.Fields) {
					fieldType = w.rowType.Fields[i].Type
				}
				sb.WriteString(fmt.Sprintf("<td>%s</td>", api.FormatField(fieldType, row.GetField(i))))
			}
			sb.WriteString("</tr>")
		}

		sb.WriteString("</table>")
		sb.WriteString("</body></html>")
	} else {
		// Plain text
		sb.WriteString("SeaTunnel Data Report\n")
		sb.WriteString(fmt.Sprintf("Total Rows: %d\n", len(w.buffer)))
		sb.WriteString("=" + strings.Repeat("=", 50) + "\n\n")

		for i, row := range w.buffer {
			sb.WriteString(fmt.Sprintf("Row %d:\n", i+1))
			for j := 0; j < row.GetArity(); j++ {
				fieldName := fmt.Sprintf("Field%d", j)
				if w.rowType != nil && j < len(w.rowType.Fields) {
					fieldName = w.rowType.Fields[j].Name
				}
				var fieldType *api.SeaTunnelDataType
				if w.rowType != nil && j < len(w.rowType.Fields) {
					fieldType = w.rowType.Fields[j].Type
				}
				sb.WriteString(fmt.Sprintf("  %s: %s\n", fieldName, api.FormatField(fieldType, row.GetField(j))))
			}
			sb.WriteString("\n")
		}
	}

	return sb.String(), nil
}

// sendEmail sends the email
func (w *EmailSinkWriter) sendEmail(body string) error {
	// Create message
	m := mail.NewMsg()

	if err := m.From(w.config.FromEmail); err != nil {
		return fmt.Errorf("failed to set from address: %w", err)
	}

	if err := m.To(w.config.ToEmails...); err != nil {
		return fmt.Errorf("failed to set to addresses: %w", err)
	}

	if len(w.config.CcEmails) > 0 {
		if err := m.Cc(w.config.CcEmails...); err != nil {
			return fmt.Errorf("failed to set cc addresses: %w", err)
		}
	}

	if len(w.config.BccEmails) > 0 {
		if err := m.Bcc(w.config.BccEmails...); err != nil {
			return fmt.Errorf("failed to set bcc addresses: %w", err)
		}
	}

	m.Subject(w.config.Subject)

	if w.config.ContentType == "html" {
		m.SetBodyString(mail.TypeTextHTML, body)
	} else {
		m.SetBodyString(mail.TypeTextPlain, body)
	}

	// Create client
	var opts []mail.Option
	opts = append(opts, mail.WithPort(w.config.SMTPPort))

	if w.config.SMTPAuth {
		opts = append(opts, mail.WithSMTPAuth(mail.SMTPAuthPlain))
		opts = append(opts, mail.WithUsername(w.config.SMTPUser))
		opts = append(opts, mail.WithPassword(w.config.SMTPPassword))
	}

	if w.config.SMTPSSL {
		opts = append(opts, mail.WithSSLPort(true))
	}

	if w.config.SMTPTLS {
		opts = append(opts, mail.WithTLSPolicy(mail.TLSMandatory))
	} else {
		opts = append(opts, mail.WithTLSPolicy(mail.TLSOpportunistic))
	}

	client, err := mail.NewClient(w.config.SMTPHost, opts...)
	if err != nil {
		return fmt.Errorf("failed to create mail client: %w", err)
	}
	defer client.Close()

	if err := client.DialAndSend(m); err != nil {
		return fmt.Errorf("failed to send mail: %w", err)
	}

	return nil
}

// AbortPrepare aborts a prepared commit
func (w *EmailSinkWriter) AbortPrepare() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buffer = w.buffer[:0]
}

// SnapshotState creates a checkpoint
func (w *EmailSinkWriter) SnapshotState(checkpointID int64) ([]byte, error) {
	return nil, nil
}

// Close closes the writer
func (w *EmailSinkWriter) Close() error {
	// Send any remaining data
	if len(w.buffer) > 0 {
		if _, err := w.PrepareCommit(); err != nil {
			w.logger.Warn("Failed to send final email", zap.Error(err))
		}
	}
	return w.logger.Sync()
}
