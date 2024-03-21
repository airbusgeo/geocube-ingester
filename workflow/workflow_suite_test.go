package workflow_test

import (
	"context"
	"database/sql"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/airbusgeo/geocube-ingester/interface/database/pg"
	"github.com/airbusgeo/geocube-ingester/workflow"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// MokePublisher implements MessagePublisher
type MokePublisher struct {
	messages [][]byte
}

// Publish implements MessagePublisher
func (p *MokePublisher) Publish(ctx context.Context, data ...[]byte) (err error) {
	p.messages = append(p.messages, data...)
	return nil
}

var pgdb *sql.DB
var wf *workflow.Workflow
var ctx context.Context
var sceneQueue = MokePublisher{}
var tileQueue = MokePublisher{}

func loadSQLFile(db *sql.DB, sqlFile string) error {
	file, err := ioutil.ReadFile(sqlFile)
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
	}()
	for _, q := range strings.Split(string(file), ";") {
		q := strings.TrimSpace(q)
		if q == "" {
			continue
		}
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

var _ = BeforeSuite(func() {
	dbConnection := "postgresql://postgres:1234@localhost:5432/"
	dbName := "geocube_test"
	ctx = context.Background()
	var err error

	// Create database if not exists
	pgdb, err = sql.Open("postgres", dbConnection)
	Expect(err).NotTo(HaveOccurred())
	pgdb.Exec("CREATE DATABASE " + dbName)

	// Create schema and initialize database
	pgdb, err = sql.Open("postgres", dbConnection+dbName)
	Expect(err).NotTo(HaveOccurred())
	_, err = pgdb.Exec("DROP SCHEMA IF EXISTS public CASCADE")
	Expect(err).NotTo(HaveOccurred())
	_, err = pgdb.Exec("CREATE SCHEMA public")
	Expect(err).NotTo(HaveOccurred())
	_, err = pgdb.Exec("GRANT ALL ON SCHEMA public TO postgres")
	Expect(err).NotTo(HaveOccurred())
	err = loadSQLFile(pgdb, "../interface/database/pg/db.sql")
	Expect(err).NotTo(HaveOccurred())

	backend, err := pg.New(ctx, dbConnection+dbName)
	Expect(err).NotTo(HaveOccurred())

	wf = workflow.NewWorkflow(backend, &sceneQueue, &tileQueue, nil)
})

func TestWorkflow(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Workflow Suite")
}

var _ = AfterSuite(func() {
	_, err := pgdb.Exec("DROP SCHEMA public CASCADE")
	Expect(err).NotTo(HaveOccurred())
})
