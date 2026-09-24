package rpc

import (
	"testing"

	"github.com/pug-sh/pug/internal/gen/proto/dashboard/projects/v1/projectsv1connect"
	"github.com/pug-sh/pug/internal/gen/proto/sdk/events/v1/eventsv1connect"
	"github.com/pug-sh/pug/internal/gen/proto/shared/insights/v1/insightsv1connect"
)

func TestDeletionGateProcedureScopes(t *testing.T) {
	tests := []struct {
		name      string
		procedure string
		project   bool
	}{
		{"SDK event write", eventsv1connect.EventsServiceBatchCreateProcedure, true},
		{"project metadata write", projectsv1connect.ProjectsServiceUpdateMetaProcedure, true},
		{"project read", insightsv1connect.InsightsServiceQueryProcedure, false},
		{"project deletion owns exclusive gate", projectsv1connect.ProjectsServiceDeleteProcedure, false},
		{"deletion history read", projectsv1connect.ProjectsServiceListDeletionsProcedure, false},
		{"deletion retry owns exclusive gate", projectsv1connect.ProjectsServiceRetryDeletionProcedure, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isProjectWriteProcedure(tc.procedure); got != tc.project {
				t.Errorf("project gate = %v, want %v", got, tc.project)
			}
		})
	}
}
