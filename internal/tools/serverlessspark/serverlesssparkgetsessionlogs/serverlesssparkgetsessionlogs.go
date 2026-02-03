// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverlesssparkgetsessionlogs

import (
	"context"
	"fmt"
	"strings"
	"time"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/embeddingmodels"
	"github.com/googleapis/genai-toolbox/internal/sources"
	ss "github.com/googleapis/genai-toolbox/internal/sources/serverlessspark"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"github.com/googleapis/genai-toolbox/internal/util/parameters"
)

const resourceType = "serverless-spark-get-session-logs"

func init() {
	if !tools.Register(resourceType, newConfig) {
		panic(fmt.Sprintf("tool type %q already registered", resourceType))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type compatibleSource interface {
	GetSessionControllerClient() *dataproc.SessionControllerClient
	GetSession(context.Context, string) (map[string]any, error)
	GetSessionLogs(ctx context.Context, params ss.QueryLogsParams) ([]map[string]any, error)
}

type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Type         string   `yaml:"type" validate:"required"`
	Source       string   `yaml:"source" validate:"required"`
	Description  string   `yaml:"description"`
	AuthRequired []string `yaml:"authRequired"`
}

// validate interface
var _ tools.ToolConfig = Config{}

// ToolConfigType returns the unique name for this tool.
func (cfg Config) ToolConfigType() string {
	return resourceType
}

// Initialize creates a new Tool instance.
func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	desc := cfg.Description
	if desc == "" {
		desc = "Gets Cloud Logging logs for a Serverless Spark (aka Dataproc Serverless) session."
	}

	params := parameters.Parameters{
		parameters.NewStringParameter("session_id", "The short session ID (e.g. 'my-session')."),
		parameters.NewStringParameterWithRequired(
			"filter",
			"Cloud Logging filter query to append to the session-specific filter.",
			false,
		),
		parameters.NewBooleanParameterWithRequired("newestFirst", "Set to true for newest logs first. Defaults to oldest first.", false),
		parameters.NewStringParameterWithRequired("startTime", "Start time in RFC3339 format (e.g., 2025-12-09T00:00:00Z). Defaults to the session creation time.", false),
		parameters.NewStringParameterWithRequired("endTime", "End time in RFC3339 format (e.g., 2025-12-09T23:59:59Z). Defaults to now (or session end time if terminal).", false),
		parameters.NewBooleanParameterWithRequired("verbose", "Include additional fields (insertId, trace, spanId, httpRequest, labels, operation, sourceLocation). Defaults to false.", false),
		parameters.NewIntParameterWithDefault("limit", 20, "Maximum number of log entries to return. Default: 20."),
	}
	inputSchema, _ := params.McpManifest()

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: desc,
		InputSchema: inputSchema,
	}

	return Tool{
		Config:      cfg,
		manifest:    tools.Manifest{Description: desc, Parameters: params.Manifest()},
		mcpManifest: mcpManifest,
		Parameters:  params,
	}, nil
}

// Tool is the implementation of the tool.
type Tool struct {
	Config
	manifest    tools.Manifest
	mcpManifest tools.McpManifest
	Parameters  parameters.Parameters
}

// Invoke executes the tool's operation.
func (t Tool) Invoke(ctx context.Context, resourceMgr tools.SourceProvider, params parameters.ParamValues, accessToken tools.AccessToken) (any, error) {
	source, err := tools.GetCompatibleSource[compatibleSource](resourceMgr, t.Source, t.Name, t.Type)
	if err != nil {
		return nil, err
	}
	paramMap := params.AsMap()
	sessionID, ok := paramMap["session_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing required parameter: session_id")
	}
	if strings.Contains(sessionID, "/") {
		return nil, fmt.Errorf("session_id must be a short name without '/': %s", sessionID)
	}

	limit := 20
	if val, ok := paramMap["limit"].(int); ok && val > 0 {
		limit = val
	}

	newestFirst, _ := paramMap["newestFirst"].(bool)
	verbose, _ := paramMap["verbose"].(bool)
	filter, _ := paramMap["filter"].(string)

	startTimeStr, _ := paramMap["startTime"].(string)
	endTimeStr, _ := paramMap["endTime"].(string)

	// If times are missing, fetch session details to fill them in
	if startTimeStr == "" || endTimeStr == "" {
		session, err := source.GetSession(ctx, sessionID)
		if err != nil {
			return nil, fmt.Errorf("failed to get session details to determine time range: %w", err)
		}

		sessionData, ok := session["session"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected session response format")
		}

		if startTimeStr == "" {
			if ct, ok := sessionData["createTime"].(string); ok {
				startTimeStr = ct
			}
		}

		if endTimeStr == "" {
			state, _ := sessionData["state"].(string)
			// Check if terminal state
			terminalStates := map[string]bool{
				"SUCCEEDED":  true,
				"FAILED":     true,
				"CANCELLED":  true,
				"TERMINATED": true,
			}
			if terminalStates[state] {
				if st, ok := sessionData["stateTime"].(string); ok {
					endTimeStr = st
				}
			}

			if endTimeStr == "" {
				endTimeStr = time.Now().Format(time.RFC3339)
			}
		}
	}

	// Validate time formats if provided (already handled by RFC3339 string assignment if retrieved from session)
	if startTimeStr != "" {
		if _, err := time.Parse(time.RFC3339, startTimeStr); err != nil {
			return nil, fmt.Errorf("startTime must be in RFC3339 format: %w", err)
		}
	}
	if endTimeStr != "" {
		if _, err := time.Parse(time.RFC3339, endTimeStr); err != nil {
			return nil, fmt.Errorf("endTime must be in RFC3339 format: %w", err)
		}
	}

	queryParams := ss.QueryLogsParams{
		SessionID:   sessionID,
		Filter:      filter,
		NewestFirst: newestFirst,
		StartTime:   startTimeStr,
		EndTime:     endTimeStr,
		Verbose:     verbose,
		Limit:       limit,
	}

	return source.GetSessionLogs(ctx, queryParams)
}

func (t Tool) EmbedParams(ctx context.Context, paramValues parameters.ParamValues, embeddingModelsMap map[string]embeddingmodels.EmbeddingModel) (parameters.ParamValues, error) {
	return parameters.EmbedParams(ctx, t.Parameters, paramValues, embeddingModelsMap, nil)
}

func (t Tool) Manifest() tools.Manifest {
	return t.manifest
}

func (t Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

func (t Tool) Authorized(services []string) bool {
	return tools.IsAuthorized(t.AuthRequired, services)
}

func (t Tool) RequiresClientAuthorization(resourceMgr tools.SourceProvider) (bool, error) {
	// Client OAuth not supported, rely on ADCs.
	return false, nil
}

func (t Tool) ToConfig() tools.ToolConfig {
	return t.Config
}

func (t Tool) GetAuthTokenHeaderName(resourceMgr tools.SourceProvider) (string, error) {
	return "Authorization", nil
}

func (t Tool) GetParameters() parameters.Parameters {
	return t.Parameters
}
