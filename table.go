package ddlbuilder

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/sqlbuilder"

	"github.com/spf13/cast"
)

type Column struct {
	Name          string           `json:"name"`
	Comment       string           `json:"comment"`
	Size          int              `json:"size"`     // 数字大小
	Unsigned      bool             `json:"unsigned"` // 无符号
	Type          string           `json:"type"`     // 字符串全部使用string,内部根据MaxLength,MinLength 决定使用哪种数据库字段类型，及大小，同理 int 全部采用 int
	Default       any              `json:"default"`
	Enums         sqlbuilder.Enums `json:"enums"`
	MaxLength     int              `json:"maxLength"` // 字符串最大长度
	MinLength     int              `json:"minLength"` // 字符串最小长度
	Maximum       uint             `json:"maximum"`   // 数字最大值
	Minimum       int              `json:"minimum"`   // 数字最小值
	Primary       bool             `json:"primary"`
	AutoIncrement bool             `json:"autoIncrement"`
	OnUpdate      string           `json:"onUpdate"`
	Tags          sqlbuilder.Tags  `json:"tags"`
}

func DBColumns(fs sqlbuilder.Fields) (columns Columns, err error) {
	columns = make(Columns, 0)
	for _, f := range fs {
		column, err := DBColumn(f)
		if err != nil {
			return nil, err
		}
		columns = append(columns, *column)
	}
	return columns, nil
}

func DBColumn(f *sqlbuilder.Field) (doc *Column, err error) {
	schema := f.Schema
	if schema == nil {
		schema = new(sqlbuilder.Schema)
		f.Schema = schema
	}

	unsigned := schema.Minimum > -1 // 默认为无符号，需要符号，则最小值设置为最大负数即可
	typeMap := map[string]string{
		"int":    "int",
		"string": "string",
	}
	typ := typeMap[schema.Type.String()]
	if typ == "" {
		if schema.Minimum > 0 || schema.Maximum > 0 { // 如果规定了最小值,最大值，默认为整型
			typ = "int"
		} else {
			typ = "string"
		}
	}

	doc = &Column{
		Name:          f.DBName(),
		Comment:       schema.FullComment(),
		Unsigned:      unsigned,
		Type:          typ,
		Default:       schema.Default,
		Enums:         schema.Enums,
		MaxLength:     schema.MaxLength,
		MinLength:     schema.MinLength,
		Maximum:       schema.Maximum,
		Minimum:       schema.Minimum,
		Primary:       schema.Primary,
		AutoIncrement: schema.AutoIncrement,
		Tags:          f.GetTags(),
	}
	return doc, nil
}

type DBFunc string

type TypeReflect[T int | uint] struct {
	UpperLimit     T      `json:"upperLimit"`     //上限
	DBType         string `json:"dbType"`         //上限
	IsDefault      bool   `json:"isDefault"`      // 是否为默认类型
	Size           int    `json:"size"`           //大小
	NoDefaultValue bool   `json:"noDefaultValue"` //不可设置默认值
}

type TypeReflects[T int | uint] []TypeReflect[T]

func (a TypeReflects[T]) Len() int           { return len(a) }
func (a TypeReflects[T]) Swap(i, j int)      { a[i], a[j].UpperLimit = a[j], a[i].UpperLimit }
func (a TypeReflects[T]) Less(i, j int) bool { return a[i].UpperLimit < a[j].UpperLimit }

func (trs TypeReflects[T]) GetByUpperLimitWithDefault(upperLimit T) (tr *TypeReflect[T]) {
	if upperLimit > 0 {
		sort.Sort(trs) // 先排序（从小到大）
		for _, t := range trs {
			if t.UpperLimit >= upperLimit {
				return &t
			}
		}
	}
	tr = trs.GetDefault()
	return tr
}

func (trs TypeReflects[T]) GetByUpperLimit(upperLimit T) (tr *TypeReflect[T], ok bool) {
	sort.Sort(trs) // 先排序（从小到大）
	for _, t := range trs {
		if t.UpperLimit >= upperLimit {
			return &t, true
		}
	}
	return nil, false
}

func (trs TypeReflects[T]) GetDefault() (tr *TypeReflect[T]) {
	sort.Sort(trs) // 先排序（从小到大）
	for _, t := range trs {
		if t.IsDefault {
			return &t
		}
	}
	bigIndex := len(trs) - 1
	if bigIndex > -1 {
		return &trs[bigIndex]
	}
	return nil
}

var TypeReflectsString = TypeReflects[int]{
	{UpperLimit: 64, DBType: "char"},                                             // 小于64位往往是ID、日期等类型，长度较为固定，直接使用char 效率高
	{UpperLimit: 255, DBType: "varchar", IsDefault: true},                        // 小字符串类型，节省空间
	{UpperLimit: 65535, DBType: "TEXT", Size: -1, NoDefaultValue: true},          // size =-1 不设置大小
	{UpperLimit: 16777215, DBType: "MEDIUMTEXT", Size: -1, NoDefaultValue: true}, // size =-1 不设置大小
	{UpperLimit: 4294967295, DBType: "LONGTEXT", Size: -1, NoDefaultValue: true}, // size =-1 不设置大小
}

// 无符号整型
var TypeReflectsUnsinedInt = TypeReflects[uint]{
	{UpperLimit: 1<<8 - 1, DBType: "TINYINT", Size: 1},
	{UpperLimit: 1<<16 - 1, DBType: "SMALLINT", Size: 11},
	{UpperLimit: 1<<24 - 1, DBType: "mediumint", Size: 11},
	{UpperLimit: 1<<32 - 1, DBType: "int", Size: 11, IsDefault: true},
	{UpperLimit: 1<<64 - 1, DBType: "bigint", Size: 11},
}
var TypeReflectsInt = TypeReflects[int]{
	{UpperLimit: 1<<7 - 1, DBType: "TINYINT", Size: 1},
	{UpperLimit: 1<<15 - 1, DBType: "SMALLINT", Size: 11},
	{UpperLimit: 1<<23 - 1, DBType: "mediumint", Size: 11},
	{UpperLimit: 1<<31 - 1, DBType: "int", Size: 11, IsDefault: true},
	{UpperLimit: 1<<63 - 1, DBType: "bigint", Size: 11},
}

func (col *Column) PrimaryDDL(driver sqlbuilder.Driver) (ddl string) {
	switch driver {
	case sqlbuilder.Driver_mysql:
		if col.Primary {
			return fmt.Sprintf("PRIMARY KEY (%s)", col.Name)
		}
		return ""
	}
	err := errors.Errorf("unsport driver:%s", string(driver))
	panic(err)
}

func (col *Column) DDL(driver sqlbuilder.Driver) (ddl string) {
	switch driver {
	case sqlbuilder.Driver_mysql:
		return Column2DDLMysql(col)
	}
	err := errors.Errorf("unsport driver:%s", string(driver))
	panic(err)
}

func Column2DDLMysql(col *Column) (ddl string) {
	if col.Enums != nil {
		col.Type = col.Enums.Type()
		col.MaxLength, col.Maximum = col.Enums.MaxLengthMaximum()
		col.Default = col.Enums.Default().Key
	}

	notNil := ""
	comment := ""
	if col.Comment != "" {
		comment = fmt.Sprintf(`COMMENT "%s"`, col.Comment)
	}
	defaul := col.Default

	typ := col.Type
	switch col.Type {
	case "string":
		if col.MaxLength < 1 {
			col.MaxLength = 255
		}
		defaul = fmt.Sprintf(`"%s"`, cast.ToString(defaul)) // 增加引号
		tr := TypeReflectsString.GetByUpperLimitWithDefault(col.MaxLength)
		if tr != nil {
			typ = tr.DBType
			if tr.Size > -1 {
				typ = fmt.Sprintf("%s(%d)", typ, col.MaxLength)
			}
			if tr.NoDefaultValue {
				defaul = nil // 不容许设置默认值
			}
		}
	case "int":
		if col.Size < 1 {
			col.Size = 11
		}
		if defaul == nil {
			defaul = 0
		}
		if col.Unsigned {
			tr := TypeReflectsUnsinedInt.GetByUpperLimitWithDefault(col.Maximum)
			if tr != nil {
				typ = fmt.Sprintf("%s(%d) unsigned", tr.DBType, col.Size)
			}
		} else {
			tr := TypeReflectsInt.GetByUpperLimitWithDefault(int(col.Maximum))
			if tr != nil {
				typ = fmt.Sprintf("%s(%d)", tr.DBType, col.Size)
			}
		}

	default:
		typ = col.Type
	}
	defaulStr := ""
	if defaul != nil {
		defaulStr = fmt.Sprintf("default %s", cast.ToString(defaul))

	}

	autoIncrement := ""
	if col.AutoIncrement {
		autoIncrement = "AUTO_INCREMENT"
		defaulStr = "" // 自增不需要默认值
	}

	if col.Tags.HastTag(sqlbuilder.Tag_createdAt) {
		typ = "datetime"
		defaulStr = "default CURRENT_TIMESTAMP"
	} else if col.Tags.HastTag(sqlbuilder.Tag_updatedAt) {
		typ = "datetime"
		defaulStr = "default  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	}
	if defaul != nil {
		notNil = " not null "
	}
	ddl = fmt.Sprintf("%s %s %s %s %s %s", col.Name, typ, notNil, autoIncrement, defaulStr, comment)
	return ddl
}

type Columns []Column

func (cols Columns) DDL(driver sqlbuilder.Driver) (lines []string) {
	arr := make([]string, 0)
	for _, col := range cols {
		arr = append(arr, col.DDL(driver))
	}
	for _, col := range cols {
		arr = append(arr, col.PrimaryDDL(driver))
	}

	lines = make([]string, 0)
	for _, l := range arr {
		if strings.TrimSpace(l) == "" {
			continue
		}
		lines = append(lines, l)
	}
	return lines
}
func (cls Columns) GetByTag(tag string) (subColumns Columns) {
	subColumns = make(Columns, 0)
	for _, col := range cls {
		if col.Tags.HastTag(tag) {
			subColumns = append(subColumns, col)
		}
	}
	return subColumns
}

type Index struct {
	Columns []Column
	Unique  bool
}

func (index Index) DDL(driver sqlbuilder.Driver) (ddl string) {
	feildNames := make([]string, 0)
	if len(index.Columns) == 0 {
		return ""
	}
	for _, col := range index.Columns {
		feildNames = append(feildNames, col.Name)
	}
	key := "index"
	namePrefix := "idx"
	if index.Unique {
		key = "unique"
		namePrefix = "uk"
	}
	name := fmt.Sprintf("%s_%s", namePrefix, strings.Join(feildNames, "_"))
	return fmt.Sprintf("%s %s(%s)", key, name, strings.Join(feildNames, ","))
}

type Indexs []Index

func (indexs Indexs) DDL(driver sqlbuilder.Driver) (lines []string) {
	arr := make([]string, 0)
	for _, index := range indexs {
		arr = append(arr, index.DDL(driver))
	}
	lines = make([]string, 0)
	for _, l := range arr {
		if strings.TrimSpace(l) == "" {
			continue
		}
		lines = append(lines, l)
	}
	return lines
}

type Table struct {
	TableName string
	Driver    sqlbuilder.Driver
	Columns   Columns // 这里占时记录列名称，但是实际上不够
	Comment   string
	Indexs    Indexs
	withDrop  bool
}

func (t *Table) GetTable() string {
	return t.TableName
}
func (t *Table) WithDrop() *Table {
	t.withDrop = true
	return t
}

func (t *Table) DDL() (ddl string) {
	var w bytes.Buffer
	if t.withDrop {
		w.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n", t.GetTable()))
	}

	w.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`(\n", t.GetTable()))
	liens := t.GetColumns().DDL(t.GetDriver())
	liens = append(liens, t.Indexs.DDL(t.GetDriver())...)
	str := strings.Join(liens, ",\n")
	w.WriteString(str)
	w.WriteString("\n")
	w.WriteString(fmt.Sprintf(`)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT="%s";`, t.Comment))
	return w.String()
}
func (t *Table) SetColumns(columns ...Column) {
	t.Columns = columns
}

func (t *Table) GetColumns() (columns Columns) {
	return t.Columns
}

func (t *Table) SetDriver(driver sqlbuilder.Driver) {
	t.Driver = driver
}

func (t *Table) GetDriver() (Driver sqlbuilder.Driver) {
	return t.Driver
}
