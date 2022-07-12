package target

import (
	"container/list"
	"crypto/md5"
	"database/sql"
	"draethos.io.com/internal/interfaces"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"draethos.io.com/pkg/streams/specs"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	MySqlInsertTemplate                      = "REPLACE INTO %s (%s) values %s;\n"
	MySqlAlterTableAddPrimaryKeyTemplate     = "CREATE TABLE IF NOT EXISTS %s (%s varchar(90) NOT NULL, PRIMARY KEY (%s));\n"
	MySqlAlterTableAddUniqueKeyColumn        = "ALTER TABLE %s ADD UNIQUE(%s);\n"
	MySqlAlterTableAddColumnVarcharTemplate  = "ALTER TABLE %s ADD COLUMN %s VARCHAR(255) %s;\n"
	MySqlAlterTableAddColumnTextTemplate     = "ALTER TABLE %s ADD COLUMN %s TEXT %s;\n"
	MySqlAlterTableAddColumnIntTemplate      = "ALTER TABLE %s ADD COLUMN %s INT %s;\n"
	MySqlAlterTableAddColumnDateTemplate     = "ALTER TABLE %s ADD COLUMN %s DATETIME %s;\n"
	MySqlAlterTableAddColumnDateTimeTemplate = "ALTER TABLE %s ADD COLUMN %s DATETIME %s;\n"
	MySqlAlterTableAddColumnNumericTemplate  = "ALTER TABLE %s ADD COLUMN %s NUMERIC(12,2) %s;\n"
	MySqlAlterTableAddColumnBoolTemplate     = "ALTER TABLE %s ADD COLUMN %s BOOL NOT NULL DEFAULT false;\n"
	MySqlAlterTableAddColumnJsonbTemplate    = "ALTER TABLE %s ADD COLUMN %s JSON NULL;\n"
	MySqlVerifyHasColumn                     = "SELECT count(1) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s' AND COLUMN_NAME='%s'"
)

type mysqlTarget struct {
	sync.Mutex
	targetSpec specs.Target
	codec      interfaces.CodecInterface
	queue      *list.List
	columns    []string
	db         *sql.DB
}

func NewMysqlTarget(targetSpec specs.Target, codec interfaces.CodecInterface) (*mysqlTarget, error) {
	return &mysqlTarget{
		targetSpec: targetSpec,
		codec:      codec,
		queue:      list.New(),
		columns:    make([]string, 0),
	}, nil
}

func (p *mysqlTarget) Initialize() error {
	if _, ok := p.targetSpec.TargetSpecs.Configurations["host"].(string); !ok {
		return errors.Errorf("target host not defined")
	}

	if _, ok := p.targetSpec.TargetSpecs.Configurations["user"].(string); !ok {
		return errors.Errorf("target user not defined")
	}

	if _, ok := p.targetSpec.TargetSpecs.Configurations["password"].(string); !ok {
		return errors.Errorf("target password not defined")
	}

	if p.targetSpec.TargetSpecs.KeyColumnName == "" {
		p.targetSpec.TargetSpecs.KeyColumnName = "id"
	}

	db, err := sql.Open("mysql",
		fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
			p.targetSpec.TargetSpecs.Configurations["user"],
			p.targetSpec.TargetSpecs.Configurations["password"],
			p.targetSpec.TargetSpecs.Configurations["host"],
			p.targetSpec.TargetSpecs.Configurations["port"],
			p.targetSpec.TargetSpecs.Database))

	if err != nil {
		return errors.Errorf("failed to connect target pgsql: %s", err.Error())
	}

	p.db = db

	if _, err := p.db.Exec(
		fmt.Sprintf(
			MySqlAlterTableAddPrimaryKeyTemplate,
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

func (p *mysqlTarget) Attach(_ string, data map[string]interface{}) error {
	p.Lock()
	defer p.Unlock()

	p.queue.PushBack(data)

	zap.S().Debugf("queue size: %v", p.queue.Len())

	return nil
}

func (p *mysqlTarget) CanFlush() bool {
	p.Lock()
	defer p.Unlock()

	return p.queue.Len() >= p.targetSpec.TargetSpecs.BatchSize
}

func (p *mysqlTarget) Flush() error {
	p.Lock()
	defer p.Unlock()

	if p.queue.Len() == 0 {
		return nil
	}

	zap.S().Infof("flush %v events", p.queue.Len())

	rows := make([]map[string]string, 0)
	elementLen := p.queue.Len()
	for i := 0; i <= elementLen; i++ {
		if element := p.queue.Front(); element != nil {
			columns, _ := p.buildCommands(*element)
			p.queue.Remove(element)
			rows = append(rows, columns)
		}
	}

	inserts := make([]string, 0)
	for _, row := range rows {
		values := make([]string, 0)
		for _, field := range p.columns {
			if row[field] == "" {
				row[field] = "NULL"
			}

			values = append(values, row[field])
		}

		inserts = append(inserts, fmt.Sprintf("(%s)", strings.Join(values, ",")))
	}

	if _, err := p.db.Exec(fmt.Sprintf(
		MySqlInsertTemplate,
		p.targetSpec.TargetSpecs.Table,
		strings.Join(p.columns, ","),
		strings.Join(inserts, ","))); err != nil {
		return err
	}

	return nil
}

func (p *mysqlTarget) containColumn(key string) *int {
	for k, v := range p.columns {
		if key == v {
			return &k
		}
	}

	return nil
}

func (p *mysqlTarget) hasColumn(column string) (bool, error) {
	var qtdRows int = 0
	err := p.db.QueryRow(fmt.Sprintf(
		MySqlVerifyHasColumn,
		p.targetSpec.TargetSpecs.Table, column)).Scan(&qtdRows)
	if err != nil {
		return false, err
	}

	if qtdRows > 0 {
		return true, nil
	}

	return false, nil
}

func (p *mysqlTarget) fieldIsDate(value interface{}) bool {
	for _, pattern := range []string{
		`([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))`,
	} {
		r, _ := regexp.Compile(pattern)
		v, _ := value.(string)

		if r.MatchString(v) {
			return true
		}
	}

	return false
}

func (p *mysqlTarget) fieldIsDateTime(value interface{}) bool {
	for _, pattern := range []string{
		`([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T(0[0-9]|[24]\d|3[01]):(0[0-9]|[59]\d|3[01]):(0[0-9]|[59]\d|3[01]).(0[0-9]\d|3[01]))`,
		`([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T(0[0-9]|[24]\d|3[01]):(0[0-9]|[59]\d|3[01]):(0[0-9]|[59]\d|3[01]))`,
		`([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) (0[0-9]|[24]\d|3[01]):(0[0-9]|[59]\d|3[01]):(0[0-9]|[59]\d|3[01]).(0[0-9]\d|3[01]))`,
		`([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) (0[0-9]|[24]\d|3[01]):(0[0-9]|[59]\d|3[01]):(0[0-9]|[59]\d|3[01]))`,
	} {
		r, _ := regexp.Compile(pattern)
		v, _ := value.(string)

		if r.MatchString(v) {
			return true
		}
	}

	return false
}

func (p *mysqlTarget) buildCommands(element list.Element) (map[string]string, error) {
	content, ok := element.Value.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("failed to convert content list")
	}

	values := make(map[string]string, 0)
	if index := p.containColumn(p.targetSpec.TargetSpecs.KeyColumnName); index == nil {
		p.columns = append(p.columns, p.targetSpec.TargetSpecs.KeyColumnName)
	}

	for k, v := range content {
		if index := p.containColumn(k); index == nil {
			fieldVarcharDefault := "NULL"
			fieldNumberDefault := "NOT NULL DEFAULT 0"
			if k == p.targetSpec.TargetSpecs.KeyColumnName {
				fieldVarcharDefault = "NOT NULL"
				fieldNumberDefault = "NOT NULL"
			}

			switch v.(type) {
			case int:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnIntTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnIntTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case int8:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnIntTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnIntTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case int16:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnIntTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnIntTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case int32:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnIntTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnIntTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case int64:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnIntTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnIntTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case float32:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnNumericTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnNumericTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case float64:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnNumericTemplate, p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnNumericTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldNumberDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case bool:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnBoolTemplate, p.targetSpec.TargetSpecs.Table, k)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnBoolTemplate,
						p.targetSpec.TargetSpecs.Table, k)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case map[string]interface{}:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					zap.S().Debugf(MySqlAlterTableAddColumnJsonbTemplate, p.targetSpec.TargetSpecs.Table, k)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnJsonbTemplate,
						p.targetSpec.TargetSpecs.Table, k)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			case []interface{}:
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddColumnJsonbTemplate,
						p.targetSpec.TargetSpecs.Table, k)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			default:
				alterTableAddColumnTemplate := MySqlAlterTableAddColumnVarcharTemplate

				if p.fieldIsDate(v) {
					alterTableAddColumnTemplate = MySqlAlterTableAddColumnDateTemplate
				}

				if p.fieldIsDateTime(v) {
					alterTableAddColumnTemplate = MySqlAlterTableAddColumnDateTimeTemplate
				}

				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					if _, err := p.db.Exec(fmt.Sprintf(
						alterTableAddColumnTemplate,
						p.targetSpec.TargetSpecs.Table, k, fieldVarcharDefault)); err != nil {
						zap.S().Warnf("failed to create column %s: %s\n", k, err.Error())
					}
				}
			}

			if k == p.targetSpec.TargetSpecs.KeyColumnName {
				if exists, _ := p.hasColumn(k); !exists {
					zap.S().Infof("column %s not found, running build script...", k)
					if _, err := p.db.Exec(fmt.Sprintf(
						MySqlAlterTableAddUniqueKeyColumn,
						p.targetSpec.TargetSpecs.Table, k)); err != nil {
						zap.S().Warnf("failed to create unique key %s: %s\n", k, err.Error())
					}
				}
			}

			p.columns = append(p.columns, k)
		}

		switch v.(type) {
		case int:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values[k] = fmt.Sprintf("%v", data)
		case int8:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values[k] = fmt.Sprintf("%v", data)
		case int16:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values[k] = fmt.Sprintf("%v", data)
		case int32:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values[k] = fmt.Sprintf("%v", data)
		case int64:
			value, _ := v.(string)
			data, _ := strconv.ParseInt(value, 0, 64)
			values[k] = fmt.Sprintf("%v", data)
		case float32:
			value, _ := v.(string)
			data, _ := strconv.ParseFloat(value, 64)
			values[k] = fmt.Sprintf("%v", data)
		case float64:
			value, _ := v.(string)
			data, _ := strconv.ParseFloat(value, 64)
			values[k] = fmt.Sprintf("%v", data)
		case bool:
			value, _ := v.(string)
			data, _ := strconv.ParseBool(value)
			values[k] = fmt.Sprintf("%v", data)
		case nil:
			values[k] = fmt.Sprintf("%v", "NULL")
		case map[string]interface{}:
			data, _ := json.Marshal(v)
			values[k] = fmt.Sprintf("'%s'", data)
		case []interface{}:
			data, _ := json.Marshal(v)
			values[k] = fmt.Sprintf("'%s'", data)
		default:
			values[k] = fmt.Sprintf("'%v'", v)

			if p.fieldIsDateTime(v) {
				values[k] = strings.ReplaceAll(fmt.Sprintf("'%v'", v), "T", " ")
			}
		}
	}

	hasKey := false
	for _, col := range p.columns {
		if col == p.targetSpec.TargetSpecs.KeyColumnName {
			hasKey = true
		}
	}

	if !hasKey || values[p.targetSpec.TargetSpecs.KeyColumnName] == "" {
		values[p.targetSpec.TargetSpecs.KeyColumnName] = fmt.Sprintf("'%x'", md5.Sum([]byte(time.Now().String())))
	}

	return values, nil
}

func (p *mysqlTarget) Close() error {
	return p.db.Close()
}
