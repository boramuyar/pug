package deletion_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pug-sh/pug/internal/core/deletion"
	"github.com/pug-sh/pug/internal/testutil"
	"github.com/rs/xid"
)

type failOneMutation struct {
	driver.Conn
	failed bool
}

type noopPublisher struct{}

func (noopPublisher) Publish(context.Context, string, []byte) error { return nil }

func deletionService(pg *testutil.TestPostgres) *deletion.Service {
	return deletion.NewServiceWithPublisher(pg.PgW, nil, noopPublisher{})
}

func (c *failOneMutation) Exec(ctx context.Context, query string, args ...any) error {
	if !c.failed && strings.HasPrefix(query, "ALTER TABLE profiles DELETE") {
		c.failed = true
		return errors.New("injected ClickHouse mutation failure")
	}
	return c.Conn.Exec(ctx, query, args...)
}

func TestProjectPurgeRemovesClickHouseAndRetainsCompliance(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker Desktop")
	}
	pg := testutil.SetupPostgres(t)
	ch := testutil.SetupClickHouse(t)
	ctx := context.Background()
	orgID, projectID, actorID, complianceID := xid.New().String(), xid.New().String(), xid.New().String(), xid.New().String()
	if _, err := pg.PgW.Exec(ctx, `insert into orgs(id,display_name) values($1,'Company')`, orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.PgW.Exec(ctx, `insert into projects(id,org_id,display_name) values($1,$2,'Web')`, projectID, orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.PgW.Exec(ctx, `insert into compliance_requests(id,project_id,kind,external_id,status,completed_at) values($1,$2,'export','subject','completed',now())`, complianceID, projectID); err != nil {
		t.Fatal(err)
	}
	if err := ch.Conn.Exec(ctx, `insert into events(project_id,distinct_id,event_id,kind,occur_time,session_id) values(?,'user',generateUUIDv4(),'visit',now64(3),generateUUIDv4())`, projectID); err != nil {
		t.Fatal(err)
	}
	if err := ch.Conn.Exec(ctx, `insert into profiles(project_id,id,external_id,properties) values(?,'user','subject','{}')`, projectID); err != nil {
		t.Fatal(err)
	}
	var derivedBefore uint64
	if err := ch.Conn.QueryRow(ctx, `select count() from event_names where project_id=?`, projectID).Scan(&derivedBefore); err != nil || derivedBefore == 0 {
		t.Fatalf("event materialized view did not populate: %d, %v", derivedBefore, err)
	}
	svc := deletionService(pg)
	op, err := svc.RequestProject(ctx, actorID, orgID, projectID, "Web")
	if err != nil {
		t.Fatal(err)
	}
	if err := deletion.NewPurger(pg.PgW, ch.Conn, ch.Conn).ProcessOperation(ctx, op.ID); err == nil {
		t.Fatal("duplicate replica endpoints should fail before purge")
	} else if markErr := svc.MarkFailed(ctx, op.ID, err); markErr != nil {
		t.Fatal(markErr)
	}
	failed, err := deletion.NewService(pg.PgW).Get(ctx, op.ID)
	if err != nil || failed.Status != "failed" || len(failed.Projects) != 1 || failed.Projects[0].ClickHouseDone != nil {
		t.Fatalf("replica configuration failure was not visible: %+v, %v", failed, err)
	}
	if _, err := svc.Retry(ctx, actorID, op.ID); err != nil {
		t.Fatalf("retry after correcting replica configuration: %v", err)
	}
	if err := deletion.NewPurger(pg.PgW, ch.Conn).ProcessOperation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	got, err := deletion.NewService(pg.PgW).Get(ctx, op.ID)
	if err != nil || got.Status != "deleted" || len(got.Projects) != 1 || got.Projects[0].ClickHouseDone == nil || got.Projects[0].PostgresDone == nil {
		t.Fatalf("operation: %+v, %v", got, err)
	}
	var projects, ledger int
	if err := pg.PgW.QueryRow(ctx, `select count(*) from projects where id=$1`, projectID).Scan(&projects); err != nil {
		t.Fatal(err)
	}
	if err := pg.PgW.QueryRow(ctx, `select count(*) from compliance_requests where id=$1`, complianceID).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if projects != 0 || ledger != 1 {
		t.Fatalf("project=%d compliance ledger=%d", projects, ledger)
	}
	var events, profiles, derivedAfter uint64
	if err := ch.Conn.QueryRow(ctx, `select count() from events where project_id=?`, projectID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := ch.Conn.QueryRow(ctx, `select count() from profiles where project_id=?`, projectID).Scan(&profiles); err != nil {
		t.Fatal(err)
	}
	if err := ch.Conn.QueryRow(ctx, `select count() from event_names where project_id=?`, projectID).Scan(&derivedAfter); err != nil {
		t.Fatal(err)
	}
	if events != 0 || profiles != 0 || derivedAfter != 0 {
		t.Fatalf("events=%d profiles=%d event_names=%d", events, profiles, derivedAfter)
	}
}

func TestPartialClickHouseMutationRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker Desktop")
	}
	pg := testutil.SetupPostgres(t)
	ch := testutil.SetupClickHouse(t)
	ctx := context.Background()
	orgID, projectID, actorID := xid.New().String(), xid.New().String(), xid.New().String()
	if _, err := pg.PgW.Exec(ctx, `insert into orgs(id,display_name) values($1,'Company')`, orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.PgW.Exec(ctx, `insert into projects(id,org_id,display_name) values($1,$2,'Web')`, projectID, orgID); err != nil {
		t.Fatal(err)
	}
	if err := ch.Conn.Exec(ctx, `insert into events(project_id,distinct_id,event_id,kind,occur_time,session_id) values(?,'user',generateUUIDv4(),'visit',now64(3),generateUUIDv4())`, projectID); err != nil {
		t.Fatal(err)
	}
	if err := ch.Conn.Exec(ctx, `insert into profiles(project_id,id,external_id,properties) values(?,'user','subject','{}')`, projectID); err != nil {
		t.Fatal(err)
	}
	svc := deletionService(pg)
	op, err := svc.RequestProject(ctx, actorID, orgID, projectID, "Web")
	if err != nil {
		t.Fatal(err)
	}
	if err := deletion.NewPurger(pg.PgW, &failOneMutation{Conn: ch.Conn}).ProcessOperation(ctx, op.ID); err == nil {
		t.Fatal("injected partial mutation should fail")
	} else if markErr := svc.MarkFailed(ctx, op.ID, err); markErr != nil {
		t.Fatal(markErr)
	}
	failed, err := svc.Get(ctx, op.ID)
	if err != nil || failed.Status != "failed" || failed.Projects[0].ClickHouseDone != nil {
		t.Fatalf("partial mutation progress: %+v, %v", failed, err)
	}
	if _, err := svc.Retry(ctx, actorID, op.ID); err != nil {
		t.Fatal(err)
	}
	if err := deletion.NewPurger(pg.PgW, ch.Conn).ProcessOperation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := svc.Get(ctx, op.ID)
	if err != nil || completed.Status != "deleted" || completed.Projects[0].ClickHouseDone == nil || completed.Projects[0].PostgresDone == nil {
		t.Fatalf("retry result: %+v, %v", completed, err)
	}
}
