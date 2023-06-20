package memodb

import (
	"log"
	"reflect"
	"sync"
	"testing"

	gomock "github.com/golang/mock/gomock"
)

var (
	trx = []Transactions{
		{
			TrxID:       1234,
			TrxCustomer: "someone",
			TrxAmount:   0.1234,
			TrxTips:     0.2345,
			TrxDiscount: 0.1234,
		}, {
			TrxID:       1235,
			TrxCustomer: "someone else",
			TrxAmount:   0.1234,
			TrxTips:     0.2346,
			TrxDiscount: 0.1237,
		}, {
			TrxID:       1236,
			TrxCustomer: "someone maybe",
			TrxAmount:   0.1232,
			TrxTips:     0.2341,
			TrxDiscount: 0.1555,
		}, {
			TrxID:       1275,
			TrxCustomer: "someone idunno",
			TrxAmount:   0.1236,
			TrxTips:     0.2349,
			TrxDiscount: 0.1231,
		}, {
			TrxID:       1276,
			TrxCustomer: "someone somewhere",
			TrxAmount:   0.1215,
			TrxTips:     0.2333,
			TrxDiscount: 0.1235,
		},
	}

	trxUpdate = Transactions{
		TrxID:       1234,
		TrxCustomer: "someone",
		TrxAmount:   0.99999,
		TrxTips:     0.9999,
		TrxDiscount: 0.1234,
	}
	trxUpdated = map[string]FieldValue{}

	trxFields = []map[string]FieldValue{
		{}, {}, {}, {}, {},
	}
	trxMemost   = MemoStructure{}
	trxLabel    = `transactions`
	trxMemoName = `transactions`
)

func init() {
	trxMemost, _ = GenerateMemoStructure(trx[0])
	ToFieldValues(trx[0], `transactions`, trxFields[0])
	ToFieldValues(trx[1], `transactions`, trxFields[1])
	ToFieldValues(trx[2], `transactions`, trxFields[2])
	ToFieldValues(trx[3], `transactions`, trxFields[3])
	ToFieldValues(trx[4], `transactions`, trxFields[4])

	ToFieldValues(trxUpdate, `transactions`, trxUpdated)

	return
}

func TestMemoDB_CreateNewMemo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type fields struct {
		memos sync.Map
	}
	type args struct {
		memoname string
		memost   MemoStructure
	}
	tests := []struct {
		name    string
		fields  func() MemoDBInterface
		args    args
		wantErr bool
	}{
		{
			name: "test happy flow",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}
				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				memost:   trxMemost,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mm := tt.fields()
			if err := mm.CreateNewMemo(tt.args.memoname, tt.args.memost); (err != nil) != tt.wantErr {
				t.Errorf("MemoDB.CreateNewMemo() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemoDB_Insert(t *testing.T) {
	type fields struct {
		memos sync.Map
	}
	type args struct {
		memoname string
		field    map[string]FieldValue
	}
	tests := []struct {
		name    string
		fields  func() MemoDBInterface
		args    args
		wantErr bool
	}{
		{
			name: "test happy flow",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}

				err := mmd.CreateNewMemo(trxMemoName, trxMemost)
				if err != nil {
					log.Println(err)
				}

				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				field:    trxFields[0],
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mm := tt.fields()
			if err := mm.Insert(tt.args.memoname, tt.args.field); (err != nil) != tt.wantErr {
				t.Errorf("MemoDB.Insert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemoDB_Update(t *testing.T) {
	type fields struct {
		memos sync.Map
	}
	type args struct {
		memoname string
		field    map[string]FieldValue
	}
	tests := []struct {
		name    string
		fields  func() MemoDBInterface
		args    args
		wantErr bool
	}{
		{
			name: "test happy update flow",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}

				err := mmd.CreateNewMemo(trxMemoName, trxMemost)
				if err != nil {
					log.Println(err)
				}
				mmd.Insert(trxMemoName, trxFields[0])

				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				field:    trxUpdated,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mm := tt.fields()
			if err := mm.Update(tt.args.memoname, tt.args.field); (err != nil) != tt.wantErr {
				t.Errorf("MemoDB.Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemoDB_Query(t *testing.T) {

	type fields struct {
		memos sync.Map
	}
	type args struct {
		memoname string
		rel      QueryRelation
		params   []QueryParams
	}
	tests := []struct {
		name    string
		fields  func() MemoDBInterface
		args    args
		wantQr  QueryResult
		wantErr bool
	}{
		{
			name: "happy flow query indexed and",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}

				err := mmd.CreateNewMemo(trxMemoName, trxMemost)
				if err != nil {
					log.Println(err)
				}

				mmd.Insert(trxMemoName, trxFields[0])
				mmd.Insert(trxMemoName, trxFields[1])
				mmd.Insert(trxMemoName, trxFields[2])
				mmd.Insert(trxMemoName, trxFields[3])
				mmd.Insert(trxMemoName, trxFields[4])

				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				rel:      QueryRelationAnd,
				params: []QueryParams{
					{
						Field: FieldValue{
							Label: trxMemost.IndexedFields[0].Label,
							Type:  trxMemost.IndexedFields[0].FieldType,
							Value: `someone`,
						},
						QueryOperator: Equal,
					}, {
						Field: FieldValue{
							Label: trxMemost.IndexedFields[1].Label,
							Type:  trxMemost.IndexedFields[1].FieldType,
							Value: 0.1234,
						},
						QueryOperator: Equal,
					},
				},
			},
			wantQr: QueryResult{
				Data: []QueryHits{
					{
						Data: trxFields[0],
					},
				},
				Hits: 1,
			},
			wantErr: true,
		}, {
			name: "happy flow query key field",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}

				err := mmd.CreateNewMemo(trxMemoName, trxMemost)
				if err != nil {
					log.Println(err)
				}

				mmd.Insert(trxMemoName, trxFields[0])
				mmd.Insert(trxMemoName, trxFields[1])
				mmd.Insert(trxMemoName, trxFields[2])
				mmd.Insert(trxMemoName, trxFields[3])
				mmd.Insert(trxMemoName, trxFields[4])

				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				rel:      QueryRelationAnd,
				params: []QueryParams{
					{
						Field: FieldValue{
							Label: trxMemost.KeyField.Label,
							Type:  trxMemost.KeyField.FieldType,
							Value: int64(1234),
						},
						QueryOperator: Equal,
					},
				},
			},
			wantQr: QueryResult{
				Data: []QueryHits{
					{
						Data: trxFields[0],
					},
				},
				Hits: 1,
			},
			wantErr: false,
		}, {
			name: "happy flow search query",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}

				err := mmd.CreateNewMemo(trxMemoName, trxMemost)
				if err != nil {
					log.Println(err)
				}

				mmd.Insert(trxMemoName, trxFields[0])
				mmd.Insert(trxMemoName, trxFields[1])
				mmd.Insert(trxMemoName, trxFields[2])
				mmd.Insert(trxMemoName, trxFields[3])
				mmd.Insert(trxMemoName, trxFields[4])

				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				rel:      QueryRelationAnd,
				params: []QueryParams{
					{
						Field: FieldValue{
							Label: trxMemost.Fields[3].Label,
							Type:  trxMemost.Fields[3].FieldType,
							Value: float64(0.1235),
						},
						QueryOperator: LargerOrEqualThan,
					},
				},
			},
			wantQr: QueryResult{
				Data: []QueryHits{
					{
						Data: trxFields[1],
					}, {
						Data: trxFields[2],
					}, {
						Data: trxFields[4],
					},
				},
				Hits: 3,
			},
			wantErr: false,
		}, {
			name: "happy flow all rows query",
			fields: func() MemoDBInterface {
				mmd := MemoDB{
					memos: sync.Map{},
				}

				err := mmd.CreateNewMemo(trxMemoName, trxMemost)
				if err != nil {
					log.Println(err)
				}

				mmd.Insert(trxMemoName, trxFields[0])
				mmd.Insert(trxMemoName, trxFields[1])
				mmd.Insert(trxMemoName, trxFields[2])
				mmd.Insert(trxMemoName, trxFields[3])
				mmd.Insert(trxMemoName, trxFields[4])

				return &mmd
			},
			args: args{
				memoname: trxMemoName,
				rel:      QueryRelationAnd,
				params:   []QueryParams{},
			},
			wantQr: QueryResult{
				Data: []QueryHits{
					{
						Data: trxFields[0],
					}, {
						Data: trxFields[1],
					}, {
						Data: trxFields[2],
					}, {
						Data: trxFields[3],
					}, {
						Data: trxFields[4],
					},
				},
				Hits: 5,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mm := tt.fields()
			gotQr, err := mm.Query(tt.args.memoname, tt.args.rel, tt.args.params...)
			if (err != nil) != tt.wantErr {
				t.Errorf("MemoDB.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotQr.Hits, tt.wantQr.Hits) {
				t.Errorf("MemoDB.Query() = %v, want %v", gotQr, tt.wantQr)
			}
		})
	}
}
