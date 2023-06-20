package memodb

import (
	"reflect"
	"testing"
)

type (
	TrxInfo struct {
		TrxBranch string `memodb:"string"`
	}

	Transactions struct {
		TrxID       int64   `memodb:"int" memost:"key"`
		TrxCustomer string  `memodb:"string" memost:"index"`
		TrxAmount   float64 `memodb:"float" memost:"index"`
		TrxDiscount float64 `memodb:"float"`
		TrxTime     string  `memodb:"string"`
		TrxTips     float64 `memodb:"-"`
		TrxInfo     TrxInfo
	}
)

func TestToFieldValues(t *testing.T) {
	dests := []map[string]FieldValue{
		{}, {},
	}

	type args struct {
		data  interface{}
		label string
		dest  map[string]FieldValue
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test happy flow",
			args: args{
				data: Transactions{
					TrxID:       1234,
					TrxCustomer: "thestring",
					TrxAmount:   0.123,
					TrxTips:     0.234,
				},
				label: "transactions",
				dest:  dests[0],
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ToFieldValues(tt.args.data, tt.args.label, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("ToFieldValues() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFillFieldValues(t *testing.T) {
	dests := []Transactions{
		{},
	}

	type args struct {
		source map[string]FieldValue
		dest   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test happy flow",
			args: args{
				source: map[string]FieldValue{
					`transactions/TrxAmount`: {
						Label: `transactions/TrxAmount`,
						Type:  FieldTypeFloat64,
						Value: float64(0.123),
					},
					`transactions/TrxCustomer`: {
						Label: `transactions/TrxCustomer`,
						Type:  FieldTypeString,
						Value: `thestring`,
					},
					`transactions/TrxID`: {
						Label: `transactions/TrxID`,
						Type:  FieldTypeInt64,
						Value: int64(1234),
					},
				},
				dest: &dests[0],
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := FillFieldValues(tt.args.source, tt.args.dest); (err != nil) != tt.wantErr {
				t.Errorf("FillFieldValues() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGenerateMemoStructure(t *testing.T) {
	type args struct {
		source interface{}
	}
	tests := []struct {
		name       string
		args       args
		wantMemost MemoStructure
		wantErr    bool
	}{
		{
			name: "test happy flow",
			args: args{
				source: Transactions{},
			},
			wantMemost: MemoStructure{
				KeyField: Field{
					FieldType: 2,
					Label:     `TrxID`,
				},
				IndexedFields: []Field{
					{
						FieldType: FieldTypeString,
						Label:     `TrxCustomer`,
					},
					{
						FieldType: FieldTypeFloat64,
						Label:     `TrxAmount`,
					},
				},
				Fields: []Field{
					{
						FieldType: FieldTypeInt64,
						Label:     `TrxID`,
					}, {
						FieldType: FieldTypeString,
						Label:     `TrxCustomer`,
					}, {
						FieldType: FieldTypeFloat64,
						Label:     `TrxAmount`,
					}, {
						FieldType: FieldTypeFloat64,
						Label:     `TrxDiscount`,
					}, {
						FieldType: FieldTypeString,
						Label:     `TrxTime`,
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMemost, err := GenerateMemoStructure(tt.args.source)
			if (err != nil) != tt.wantErr {
				t.Errorf("GenerateMemoStructure() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotMemost, tt.wantMemost) {
				t.Errorf("GenerateMemoStructure() = %+v, want %+v", gotMemost, tt.wantMemost)
			}
		})
	}
}
