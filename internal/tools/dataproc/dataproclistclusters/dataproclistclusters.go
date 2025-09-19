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

package dataproclistclusters

import (
	"context"
	"fmt"

	dataprocpb "cloud.google.com/go/dataproc/apiv1/dataprocpb"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/dataproc"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"google.golang.org/api/iterator"
)

const kind = "dataproc-list-clusters"

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

	ds, ok := rawS.(*dataproc.Source)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be `%s`", kind, dataproc.SourceKind)
	}

	desc := cfg.Description
	if desc == "" {
		desc = "Lists all available Dataproc Spark clusters"
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

// Tool is the implementation of the dataproc tool.
type Tool struct {
	Name        string `yaml:"name"`
	Kind        string `yaml:"kind"`
	Description string `yaml:"description"`

	Source *dataproc.Source

	manifest    tools.Manifest
	mcpManifest tools.McpManifest
}

// Invoke executes the tool's operation.
func (t Tool) Invoke(ctx context.Context, params tools.ParamValues, accessToken tools.AccessToken) (any, error) {
	req := &dataprocpb.ListClustersRequest{
		ProjectId: t.Source.Project,
		Region:    t.Source.Region,
	}

	var clusters []*dataprocpb.Cluster
	it := t.Source.Client.ListClusters(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing Dataproc clusters: %w", err)
		}
		clusters = append(clusters, resp)
	}

	return clusters, nil
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
