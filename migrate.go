package ddlbuilder

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/suifengpiao14/sqlbuilder"
)

const (
	SCENE_DDL_CREATE sqlbuilder.Scene = "create"
	SCENE_DDL_MODIFY sqlbuilder.Scene = "modify"
	SCENE_DDL_APPEND sqlbuilder.Scene = "append"
	SCENE_DDL_DELETE sqlbuilder.Scene = "delete"
)

type Token string //ddl 语法部分token

func (t Token) IsSame(target Token) bool {
	return strings.EqualFold(string(t), string(target))
}

type Migrate struct {
	Dialect sqlbuilder.Driver
	Scene   sqlbuilder.Scene
	Options []MigrateOptionI
	DDL     string
}

type Migrates []Migrate

func (ms Migrates) GetByScene(driver sqlbuilder.Driver, scene sqlbuilder.Scene) (subMs Migrates) {
	subMs = make(Migrates, 0)
	for _, m := range ms {
		if driver.IsSame(m.Dialect) && scene.Is(m.Scene) {
			subMs = append(subMs, m)
		}
	}
	return subMs
}
func (ms Migrates) DDLs() (ddls []string) {
	ddls = make([]string, 0)
	for _, m := range ms {
		ddls = append(ddls, m.DDL)
	}

	return ddls
}

func (ms Migrates) String() string {
	w := bytes.Buffer{}
	for _, m := range ms {
		w.WriteString(m.DDL)
		w.WriteString("\n")
	}
	return w.String()
}

type MigrateOptionI interface {
	String() string
	Driver() sqlbuilder.Driver
	Token() Token
}

func GetMigrateOpion(target MigrateOptionI, ops ...MigrateOptionI) MigrateOptionI {
	for _, op := range ops {
		if op.Driver().IsSame(target.Driver()) && op.Token().IsSame(target.Token()) {
			return op
		}
	}
	return target
}

type _MysqlAfter struct {
	filedName string
}

const (
	Mysql_Token_after Token = "AFTER"
)

func (o _MysqlAfter) Driver() sqlbuilder.Driver {
	return sqlbuilder.Driver_mysql
}
func (o _MysqlAfter) Token() Token {
	return Mysql_Token_after
}

func (o _MysqlAfter) String() string {
	if o.filedName == "" {
		return ""
	}
	return fmt.Sprintf("AFTER `%s`", o.filedName)
}

func MigrateOptionMysqlAfter(fieldName string) MigrateOptionI {
	return _MysqlAfter{
		filedName: fieldName,
	}
}
