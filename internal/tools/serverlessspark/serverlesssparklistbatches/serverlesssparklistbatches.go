// Copyright 2025 Google LLC
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

package serverlesssparklistbatches

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/serverlessspark"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

const kind = "serverless-spark-list-batches"

func init() {
	if !tools.Register(kind, newConfig) {
		panic(fmt.Sprintf("tool kind %q already registered", kind))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source" validate:"required"`
	Description  string   `yaml:"description"`
	AuthRequired []string `yaml:"authRequired"`
}

// validate interface
var _ tools.ToolConfig = Config{}

// ToolConfigKind returns the unique name for this tool.
func (cfg Config) ToolConfigKind() string {
	return kind
}

// Initialize creates a new Tool instance.
func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	rawS, ok := srcs[cfg.Source]
	if !ok {
		return nil, fmt.Errorf("source %q not found", cfg.Source)
	}

	ds, ok := rawS.(*serverlessspark.Source)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be `%s`", kind, serverlessspark.SourceKind)
	}

	desc := cfg.Description
	if desc == "" {
		desc = "Lists available Serverless Spark (aka Dataproc Serverless) batches"
	}

	// An empty parameters object will generate the correct empty schema.
	allParameters := tools.Parameters{}

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: desc,
		InputSchema: allParameters.McpManifest(),
	}

	return Tool{
		Name:        cfg.Name,
		Kind:        kind,
		Source:      ds,
		manifest:    tools.Manifest{Description: desc, Parameters: allParameters.Manifest()},
		mcpManifest: mcpManifest,
	}, nil
}

// Tool is the implementation of the tool.
type Tool struct {
	Name        string `yaml:"name"`
	Kind        string `yaml:"kind"`
	Description string `yaml:"description"`

	Source *serverlessspark.Source

	manifest    tools.Manifest
	mcpManifest tools.McpManifest
}

// apiBatch represents a single batch job from the API.
type apiBatch struct {
	Name       string `json:"name"`
	UUID       string `json:"uuid"`
	State      string `json:"state"`
	Creator    string `json:"creator"`
	CreateTime string `json:"createTime"`
}

// apiListBatchesResponse is the response from the list batches API.
type apiListBatchesResponse struct {
	Batches       []apiBatch `json:"batches"`
	NextPageToken string     `json:"nextPageToken"`
}

// Invoke executes the tool's operation.
func (t Tool) Invoke(ctx context.Context, params tools.ParamValues, accessToken tools.AccessToken) (any, error) {
	url := fmt.Sprintf("%s/v1/projects/%s/locations/%s/batches", t.Source.BaseURL, t.Source.Project, t.Source.Location)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("User-Agent", t.Source.UserAgent)

	client, err := t.Source.GetClient(ctx, string(accessToken))
	if err != nil {
		return nil, fmt.Errorf("error getting client: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var listResponse apiListBatchesResponse
	if err := json.NewDecoder(resp.Body).Decode(&listResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	type SimpleBatch struct {
		Name       string `json:"name"`
		UUID       string `json:"uuid"`
		State      string `json:"state"`
		Creator    string `json:"creator"`
		CreateTime string `json:"createTime"`
	}

	simpleBatches := make([]SimpleBatch, 0, len(listResponse.Batches))
	for _, batch := range listResponse.Batches {
		simpleBatches = append(simpleBatches, SimpleBatch(batch))
	}

	type SimpleListBatchesResponse struct {
		Batches       []SimpleBatch `json:"batches"`
		NextPageToken string        `json:"nextPageToken,omitempty"`
	}

	return SimpleListBatchesResponse{
		Batches:       simpleBatches,
		NextPageToken: listResponse.NextPageToken,
	}, nil
}

// ParseParams parses and validates the input parameters.
func (t Tool) ParseParams(data map[string]any, claims map[string]map[string]any) (tools.ParamValues, error) {
	return nil, nil
}

// Manifest returns the tool's manifest.
func (t Tool) Manifest() tools.Manifest {
	return t.manifest
}

// McpManifest returns the tool's MCP manifest.
func (t Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

// Authorized checks if the tool is authorized to run.
func (t Tool) Authorized(services []string) bool {
	return true
}

func (t Tool) RequiresClientAuthorization() bool {
	return false
}
