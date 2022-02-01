package target

import (
	"container/list"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	SqlInsertTemplate                     = "INSERT INTO %s (%s) values (%s) ON CONFLICT (%s) DO NOTHING;\n"
	SqlAlterTableAddPrimaryKeyTemplate    = "CREATE TABLE IF NOT EXISTS %s (%s varchar(90) NOT NULL, PRIMARY KEY (%s));\n"
	SqlAlterTableAddUniqueKeyColumn       = "ALTER TABLE %s ADD UNIQUE(%s);\n"
	SqlAlterTableAddColumnVarcharTemplate = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s VARCHAR(255) %s;\n"
	SqlAlterTableAddColumnTextTemplate    = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT %s;\n"
	SqlAlterTableAddColumnIntTemplate     = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s INT %s;\n"
	SqlAlterTableAddColumnNumericTemplate = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s NUMERIC(12,2) %s;\n"
	SqlAlterTableAddColumnBoolTemplate    = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s BOOL NOT NULL DEFAULT false;\n"
	SqlAlterTableAddColumnJsonbTemplate   = "ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s JSONB NULL;\n"
)

type pgsqlTarget struct {
	sync.Mutex
	targetSpec specs.Target
	codec      interfaces.CodecInterface
	queue      *list.List
	columns    map[string]bool
	db         *sql.DB
}

func NewPgsqlTarget(targetSpec specs.Target, codec interfaces.CodecInterface) (*pgsqlTarget, error) {
	return &pgsqlTarget{
		targetSpec: targetSpec,
		codec:      codec,
		queue:      list.New(),
		columns:    map[string]bool{},
	}, nil
}

func (p *pgsqlTarget) Initialize() error {
	if _, ok := p.targetSpec.TargetSpecs.Configurations["host"].(string); !ok {
		return errors.Errorf("target host not defined")
	}

	if _, ok := p.targetSpec.TargetSpecs.Configurations["user"].(string); !ok {
		return errors.Errorf("target user not defined")
	}

	if _, ok := p.targetSpec.TargetSpecs.Configurations["password"].(string); !ok {
		return errors.Errorf("target password not defined")
	}

	if _, ok := p.targetSpec.TargetSpecs.Configurations["sslmode"].(string); !ok {
		return errors.Errorf("target sslmode not defined")
	}

	if p.targetSpec.TargetSpecs.KeyColumnName == "" {
		p.targetSpec.TargetSpecs.KeyColumnName = "id"
	}

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
			p.targetSpec.TargetSpecs.Configurations["host"],
			p.targetSpec.TargetSpecs.Configurations["port"],
			p.targetSpec.TargetSpecs.Configurations["user"],
			p.targetSpec.TargetSpecs.Configurations["password"],
			p.targetSpec.TargetSpecs.Database,
			p.targetSpec.TargetSpecs.Configurations["sslmode"]))

	if err != nil {
		return errors.Errorf("failed to connect target pgsql: %s", err.Error())
	}

	p.db = db

	if _, err := p.db.Exec(
		fmt.Sprintf(
			SqlAlterTableAddPrimaryKeyTemplate,
			p.targetSpec.TargetSpecs.Table,
			p.targetSpec.TargetSpecs.KeyColumnName,
			p.targetSpec.TargetSpecs.KeyColumnName)); err != nil {
		return errors.Errorf("failed to initialize table %s: %s",
			p.targetSpec.TargetSpecs.Table,
			err.Error())
	}

	zap.S().Infof("initialize target table %s with primary key %s",
		p.targetSpec.TargetSpecs.Table,
		p.targetSpec.TargetSpecs.KeyColumnName)

	return nil
}

func (p *pgsqlTarget) Attach(_ string, data map[string]interface{}) error {
	p.Lock()
	defer p.Unlock()

	p.queue.PushBack(data)

	zap.S().Debugf("queue size: %v", p.queue.Len())

	return nil
}

func (p *pgsqlTarget) CanFlush() bool {
	p.Lock()
	defer p.Unlock()

	return p.queue.Len() >= p.targetSpec.TargetSpecs.BatchSize
}

func (p *pgsqlTarget) Flush() error {
	p.Lock()
	defer p.Unlock()

	if p.queue.Len() == 0 {
		return nil
	}

	zap.S().Infof("flush %v events", p.queue.Len())

	var bufferRx strings.Builder
	elementLen := p.queue.Len()
	for i := 0; i <= elementLen; i++ {
		if element := p.queue.Front(); element != nil {
			p.buildCommands(&bufferRx, *element)
			p.queue.Remove(element)
		}
	}

	zap.S().Debugf("sql: \n%s", bufferRx.String())

	if _, err := p.db.Exec(bufferRx.String()); err != nil {
		return err
	}

	return nil
}

func (p *pgsqlTarget) buildCommands(bufferRx *strings.Builder, element list.Element) error {
	content, ok := element.Value.(map[string]interface{})
	if !ok {
		return errors.Errorf("failed to convert content list")
	}

	columns := make([]string, 0)
	values := make([]string, 0)
	for k, v := range content {
		if _, ok := p.columns[k]; !ok {
			fieldVarcharDefault := "NULL"
			fieldNumberDefault := "NOT NULL DEFAULT 0"
			if k == p.targetSpec.TargetSpecs.KeyColumnName {
				fieldVarcharDefault = "NOT NULL"
				fieldNumberDefault = "NOT NULL"
			}

			switch v.(type) {
			case int:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnIntTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case int8:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnIntTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case int16:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnIntTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case int32:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnIntTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case int64:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnIntTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case float32:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnNumericTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case float64:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnIntTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault))
			case bool:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnBoolTemplate,
					p.targetSpec.TargetSpecs.Table, k))
			case map[string]interface{}:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnJsonbTemplate,
					p.targetSpec.TargetSpecs.Table, k))
			case []interface{}:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnJsonbTemplate,
					p.targetSpec.TargetSpecs.Table, k))
			default:
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddColumnVarcharTemplate,
					p.targetSpec.TargetSpecs.Table, k, fieldVarcharDefault))
			}

			if k == p.targetSpec.TargetSpecs.KeyColumnName {
				bufferRx.WriteString(fmt.Sprintf(
					SqlAlterTableAddUniqueKeyColumn,
					p.targetSpec.TargetSpecs.Table, k))
			}

			p.columns[k] = true
		}

		columns = append(columns, k)

		switch v.(type) {
		case int:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case int8:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case int16:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case int32:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case int64:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case float32:
			value, _ := v.(string)
			data, _ := strconv.ParseFloat(value, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case float64:
			value, _ := v.(string)
			data, _ := strconv.ParseFloat(value, 64)
			values = append(values, fmt.Sprintf("%v", data))
		case bool:
			value, _ := v.(string)
			data, _ := strconv.ParseBool(value)
			values = append(values, fmt.Sprintf("%v", data))
		case nil:
			values = append(values, "null")
		case map[string]interface{}:
			data, _ := json.Marshal(v)
			values = append(values, fmt.Sprintf("'%s", data))
		case []interface{}:
			data, _ := json.Marshal(v)
			values = append(values, fmt.Sprintf("'%s'", data))
		default:
			values = append(values, fmt.Sprintf("'%v'", v))
		}
	}

	hasKey := false
	for _, v := range columns {
		if v == p.targetSpec.TargetSpecs.KeyColumnName {
			hasKey = true
		}
	}

	if !hasKey {
		columns = append(columns, p.targetSpec.TargetSpecs.KeyColumnName)
		values = append(values, fmt.Sprintf("'%x'", md5.Sum([]byte(strings.Join(values, ",")))))
	}

	bufferRx.WriteString(fmt.Sprintf(
		SqlInsertTemplate,
		p.targetSpec.TargetSpecs.Table,
		strings.Join(columns, ","),
		strings.Join(values, ","),
		p.targetSpec.TargetSpecs.KeyColumnName))

	return nil
}

func (p *pgsqlTarget) Close() error {
	return p.db.Close()
}
