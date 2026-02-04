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

package serverlesssparkgetbatchlogs

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

const resourceType = "serverless-spark-get-batch-logs"

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
	GetBatchControllerClient() *dataproc.BatchControllerClient
	GetBatch(context.Context, string) (map[string]any, error)
	GetBatchLogs(ctx context.Context, batchID string, params ss.QueryLogsParams) ([]map[string]any, error)
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
		desc = "Gets Cloud Logging logs for a Serverless Spark (aka Dataproc Serverless) batch."
	}

	params := parameters.Parameters{
		parameters.NewStringParameter("batch_id", "The short batch ID (e.g. 'my-batch')."),
	}
	params = append(params, ss.QueryLogsParameters...)
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
	batchID, ok := paramMap["batch_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing required parameter: batch_id")
	}
	if strings.Contains(batchID, "/") {
		return nil, fmt.Errorf("batch_id must be a short name without '/': %s", batchID)
	}

	queryParams, err := ss.ParseQueryLogsParams(params)
	if err != nil {
		return nil, err
	}

	// If times are missing, fetch batch details to fill them in
	if queryParams.StartTime == "" || queryParams.EndTime == "" {
		batch, err := source.GetBatch(ctx, batchID)
		if err != nil {
			return nil, fmt.Errorf("failed to get batch details to determine time range: %w", err)
		}

		batchData, ok := batch["batch"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected batch response format")
		}

		if queryParams.StartTime == "" {
			if ct, ok := batchData["createTime"].(string); ok {
				queryParams.StartTime = ct
			}
		}

		if queryParams.EndTime == "" {
			state, _ := batchData["state"].(string)
			// Check if terminal state
			terminalStates := map[string]bool{
				"SUCCEEDED": true,
				"FAILED":    true,
				"CANCELLED": true,
			}
			if terminalStates[state] {
				if st, ok := batchData["stateTime"].(string); ok {
					queryParams.EndTime = st
				}
			}

			if queryParams.EndTime == "" {
				queryParams.EndTime = time.Now().Format(time.RFC3339)
			}
		}
	}

	// Validate time formats if provided (already handled by RFC3339 string assignment if retrieved from batch)
	// ParseQueryLogsParams already validates user input, but we might have updated them from batch details.
	// However, batch details from API should be valid. We only need to check if we modified them or if they came from user and ParseQueryLogsParams already passed.
	// Actually ParseQueryLogsParams checks user input. If we populate from API it should be fine.
	// But let's re-validate to be safe if we want, or trust ParseQueryLogsParams + API.
	// The original code re-parsed user input dates. ParseQueryLogsParams does that.
	// So we only need to worry if we *set* them here.

	return source.GetBatchLogs(ctx, batchID, queryParams)
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
