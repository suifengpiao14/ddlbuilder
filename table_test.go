package ddlbuilder_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/ddlbuilder"
	"github.com/suifengpiao14/sqlbuilder"
)

const (
	Table = "t_profile"
)

type CUTimeFields struct {
	CreateTime sqlbuilder.FieldFn[string]
}

func (f CUTimeFields) Builder() CUTimeFields {
	return CUTimeFields{
		CreateTime: func(value string) *sqlbuilder.Field {
			return sqlbuilder.NewField("").SetName("createAt")
		},
	}
}

type ProfileFields struct {
	Id       sqlbuilder.FieldFn[int]
	Nickname sqlbuilder.FieldFn[string]
	Gender   sqlbuilder.FieldFn[string]
	Email    sqlbuilder.FieldFn[string]

	CUTimeFields
	Times []CUTimeFields
}

func (ProfileFields) Builder() ProfileFields {
	Times := make([]CUTimeFields, 0)
	Times = append(Times, new(CUTimeFields).Builder())
	pf := ProfileFields{
		Id: func(value int) *sqlbuilder.Field {
			return sqlbuilder.NewField(value).SetName("id")
		},
		Nickname: func(value string) *sqlbuilder.Field {
			return sqlbuilder.NewField(value).SetName("nickname")
		},
		Gender: func(value string) *sqlbuilder.Field {
			return sqlbuilder.NewField(value).SetName("gender")
		},

		CUTimeFields: new(CUTimeFields).Builder(),
		Times:        Times,
	}
	return pf
}
func TestProfileDDL(t *testing.T) {

	profileFields := new(ProfileFields).Builder()

	fields := sqlbuilder.FieldStructToArray(profileFields, nil, nil)
	dbColumns, err := ddlbuilder.DBColumns(fields)
	require.NoError(t, err)
	table := ddlbuilder.Table{
		TableName: "t_profile",
		Driver:    sqlbuilder.Driver_mysql,
		Columns:   dbColumns,
		Comment:   "个人简介",
	}
	ddl := table.DDL()
	fmt.Println(ddl)
}
