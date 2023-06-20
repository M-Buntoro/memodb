package memodb

import (
	"errors"
	"sync"
)

var (
	ErrNotImplemented        = errors.New(`function not implemented yet`)
	ErrDuplicateMemo         = errors.New(`duplicate memo name`)
	ErrMemoNotFound          = errors.New(`memo not found`)
	ErrInsertKeyFieldMissing = errors.New(`key field missing`)
	ErrInsertDuplicateKey    = errors.New(`duplicate key field found`)
	ErrUpdateKeyNotFound     = errors.New(`key not found for update`)
)

const (
	MemodbTagInt      = `int`
	MemodbTagIntSlice = `intslice`
	MemodbTagString   = `string`
	MemodbTagFloat    = `float`
	MemodbTagIgnored  = `-`

	MemoStructureKey   = `key`
	MemoStructureIndex = `index`
	MemoStructureField = `field`
)

type (
	memo struct {
		Label    string
		datas    sync.Map // syncmap[keyfieldvalue] map[string]FieldValue{}
		indexes  sync.Map // syncmap[fieldname] map[interface{}] interface{}
		keyField Field    // key field
		fields   []Field
	}

	MemoStructure struct {
		RootLabel     string
		KeyField      Field
		IndexedFields []Field
		Fields        []Field
	}

	MemoDB struct {
		memos sync.Map
	}

	MemoDBInterface interface {
		CreateNewMemo(memoname string, memost MemoStructure) error
		Insert(memoname string, field map[string]FieldValue) error
		Update(memoname string, field map[string]FieldValue) error
		Query(memoname string, rel QueryRelation, params ...QueryParams) (qr QueryResult, err error)
	}
)

func (mem *memo) fieldByName(fieldname string) string {
	return mem.Label + `/` + fieldname
}

func (mem *memo) isIndexedQuery(qp QueryParams) (index sync.Map, isIndexed bool) {
	if qp.QueryOperator <= 0 {
		qp.QueryOperator = Equal
	}

	if mem.keyField.Label == qp.Field.Label {
		return mem.datas, true
	}

	idx, isIndexed := mem.indexes.Load(qp.Field.Label)
	if isIndexed && qp.QueryOperator == Equal {
		index = idx.(sync.Map)
	}
	return
}

type (
	QueryOperator int
	QueryRelation int

	QueryParams struct {
		Field         FieldValue
		QueryOperator QueryOperator
	}

	QueryResult struct {
		Data []QueryHits
		Hits int64
	}

	QueryHits struct {
		Data map[string]FieldValue
	}
)

const (
	QueryRelationAnd QueryRelation = iota + 1
	QueryRelationOr
)

const (
	Equal QueryOperator = iota + 1
	LargerThan
	LessThan
	LargerOrEqualThan
	LessOrEqualThan
)

func NewMemo() MemoDBInterface {
	memo := MemoDB{}
	return &memo
}

func (mm *MemoDB) CreateNewMemo(memoname string, memost MemoStructure) error {
	if _, ok := mm.memos.Load(memoname); ok {
		return ErrDuplicateMemo
	}

	newmemo := memo{
		Label:    memoname,
		datas:    sync.Map{},
		indexes:  sync.Map{},
		keyField: memost.KeyField,
		fields:   memost.Fields,
	}
	for _, v := range memost.IndexedFields {
		newmemo.indexes.Store(v.Label, sync.Map{})
	}

	mm.memos.Store(memoname, newmemo)
	return nil
}

func (mm *MemoDB) Insert(memoname string, field map[string]FieldValue) error {
	temp, ok := mm.memos.Load(memoname)
	if !ok {
		return ErrMemoNotFound
	}

	//check key field exists
	mem := temp.(memo)
	if _, ok := field[mem.fieldByName(mem.keyField.Label)]; !ok {
		return ErrInsertKeyFieldMissing
	}

	key := field[mem.fieldByName(mem.keyField.Label)]
	keyVal, err := key.ToValue()
	if err != nil {
		return err
	}

	if _, ok := mem.datas.Load(keyVal); ok {
		return ErrInsertDuplicateKey
	}

	mem.datas.Store(keyVal, field)
	mem.indexes.Range(func(k, v interface{}) bool {
		idx := v.(sync.Map)
		idxKey := k.(string)
		idxFieldVal, _ := field[mem.fieldByName(idxKey)].ToValue()

		idxItf, ok := idx.Load(idxFieldVal)
		idxSlice := map[string]bool{}
		if ok {
			idxSlice = idxItf.(map[string]bool)
		}
		idxSlice[keyVal] = true

		idx.Store(idxFieldVal, idxSlice)
		mem.indexes.Store(k, idx)
		return true
	})

	mm.memos.Store(memoname, mem)
	return nil
}

func (mm *MemoDB) Update(memoname string, field map[string]FieldValue) error {
	temp, ok := mm.memos.Load(memoname)
	if !ok {
		return ErrMemoNotFound
	}

	//check key field exists
	mem := temp.(memo)
	if _, ok := field[mem.fieldByName(mem.keyField.Label)]; !ok {
		return ErrInsertKeyFieldMissing
	}

	key := field[mem.fieldByName(mem.keyField.Label)]
	keyVal, err := key.ToValue()
	if err != nil {
		return err
	}

	prev, ok := mem.datas.Load(keyVal)
	if !ok {
		return ErrUpdateKeyNotFound
	}

	mem.datas.Delete(keyVal)
	mem.datas.Store(keyVal, field)
	mem.indexes.Range(func(k, v interface{}) bool {
		idx := v.(sync.Map)
		idxKey := k.(string)
		idxFieldVal, _ := field[mem.fieldByName(idxKey)].ToValue()

		prevVal := prev.(map[string]FieldValue)
		prevKey, _ := prevVal[mem.fieldByName(idxKey)].ToValue()

		//deletes previous index entry
		prevIdxItf, _ := idx.Load(prevKey)
		prevIdx := prevIdxItf.(map[string]bool)
		delete(prevIdx, prevKey)
		idx.Store(prevKey, prevIdx)

		//set new index entry
		newIdxItf, ok := idx.Load(keyVal)
		newIdx := map[string]bool{}
		if ok {
			newIdx = newIdxItf.(map[string]bool)
		}
		newIdx[keyVal] = true
		idx.Store(idxFieldVal, newIdx)
		mem.indexes.Store(k, idx)
		return true
	})

	mm.memos.Store(memoname, mem)
	return nil
}

func (mm *MemoDB) Query(memoname string, rel QueryRelation, params ...QueryParams) (qr QueryResult, err error) {
	temp, ok := mm.memos.Load(memoname)
	if !ok {
		return qr, ErrMemoNotFound
	}

	hits := []map[string]QueryHits{}
	mem := temp.(memo)

	if len(params) == 0 {
		hit := map[string]QueryHits{}
		mem.datas.Range(func(k, v interface{}) bool {
			data := v.(map[string]FieldValue)
			keyVal, _ := data[mem.fieldByName(mem.keyField.Label)].ToValue()
			hit[keyVal] = QueryHits{
				Data: data,
			}
			return true
		})
		hits = append(hits, hit)
	}

	for _, qparam := range params {
		idx, isIndexed := mem.isIndexedQuery(qparam)
		if isIndexed {
			hit, err := mem.queryByExactIndex(idx, qparam)
			if err != nil {
				return qr, err
			}
			hits = append(hits, hit)
		} else {
			hit, err := mem.queryBySearch(qparam)
			if err != nil {
				return qr, err
			}
			hits = append(hits, hit)
		}
	}

	var result = map[string]QueryHits{}

	// union or intersect as needed, currently supports two method :
	// Query AND are considered as A and B and C and ...etc and Z
	// Query OR  are considered as A or  B or  C or  ...etc or  Z
	for i := 0; i < len(hits); i++ {
		if rel == QueryRelationAnd {
			for k, v := range hits[i] {
				if i > 0 {
					if _, ok := result[k]; ok {
						result[k] = v
					}
				} else {
					result[k] = v
				}
			}
		} else {
			for k, v := range hits[i] {
				result[k] = v
			}
		}
	}

	for _, v := range result {
		qr.Data = append(qr.Data, v)
	}
	qr.Hits = int64(len(result))
	return
}

func (mem *memo) queryByExactIndex(index sync.Map, toQuery QueryParams) (hits map[string]QueryHits, err error) {
	var (
		data  []interface{}
		found interface{}
		hit   bool
	)
	hits = map[string]QueryHits{}
	toQueryValue, err := toQuery.Field.ToValue()
	if err != nil {
		return hits, err
	}
	if toQuery.Field.Label == mem.keyField.Label {
		found, hit = mem.datas.Load(toQueryValue)
		data = append(data, found)
	} else {
		var idxKey interface{}
		idxKey, hit = index.Load(toQueryValue)
		if hit {
			idxKeyStr := idxKey.(map[string]bool)
			for k := range idxKeyStr {
				found, hit = mem.datas.Load(k)
				data = append(data, found)
			}
		}
	}

	if hit {
		for _, dataFound := range data {
			dataField := dataFound.(map[string]FieldValue)
			dataKeyValue, err := dataField[mem.fieldByName(mem.keyField.Label)].ToValue()
			if err != nil {
				return hits, err
			}
			hits[dataKeyValue] = QueryHits{
				Data: dataField,
			}
		}

		return hits, nil
	}
	return
}

func (mem *memo) queryBySearch(toQuery QueryParams) (hits map[string]QueryHits, err error) {
	hits = map[string]QueryHits{}
	mem.datas.Range(func(k, v interface{}) bool {
		data := v.(map[string]FieldValue)
		if field, ok := data[mem.fieldByName(toQuery.Field.Label)]; ok {
			ok, err = field.Eval(toQuery)
			if ok {
				keyVal, _ := data[mem.fieldByName(mem.keyField.Label)].ToValue()
				hits[keyVal] = QueryHits{
					Data: data,
				}
			}
		}
		return true
	})
	return
}
