package memodb

import (
	"fmt"
	"reflect"
)

const (
	FieldTypeFloat64 FieldType = iota + 1
	FieldTypeInt64
	FieldTypeString
)

type (
	FieldType int

	FieldValue struct {
		Label string
		Type  FieldType
		Value interface{}
	}

	Field struct {
		FieldType FieldType
		Label     string
	}
)

func (f FieldValue) ToValue() (string, error) {
	var err error
	defer recoverPanic(&err)

	switch f.Type {
	case FieldTypeInt64:
		intVal, err := f.ToInt64()
		return fmt.Sprintf("%d", intVal), err
	case FieldTypeFloat64:
		fltVal, err := f.ToFloat64()
		return fmt.Sprintf("%f", fltVal), err
	default:
		strVal, err := f.ToString()
		return strVal, err
	}
}

func (f FieldValue) ToInt64() (int64, error) {
	var err error
	defer recoverPanic(&err)
	return f.Value.(int64), err
}
func (f FieldValue) ToFloat64() (float64, error) {
	var err error
	defer recoverPanic(&err)
	return f.Value.(float64), err
}
func (f FieldValue) ToString() (string, error) {
	var err error
	defer recoverPanic(&err)
	return f.Value.(string), err
}

func (f FieldValue) Eval(param QueryParams) (bool, error) {
	var err error
	defer recoverPanic(&err)

	//evaluates if field given matches query parameters
	switch param.QueryOperator {
	case LessThan:
		switch param.Field.Type {
		case FieldTypeInt64:
			left, err := f.ToInt64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToInt64()
			if err != nil {
				return false, err
			}
			return left < right, nil

		case FieldTypeFloat64:
			left, err := f.ToFloat64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToFloat64()
			if err != nil {
				return false, err
			}
			return left < right, nil
		default:
			left, err := f.ToString()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToString()
			if err != nil {
				return false, err
			}
			return left < right, nil
		}
	case LessOrEqualThan:
		switch param.Field.Type {
		case FieldTypeInt64:
			left, err := f.ToInt64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToInt64()
			if err != nil {
				return false, err
			}
			return left <= right, nil
		case FieldTypeFloat64:
			left, err := f.ToFloat64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToFloat64()
			if err != nil {
				return false, err
			}
			return left <= right, nil
		default:
			left, err := f.ToString()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToString()
			if err != nil {
				return false, err
			}
			return left <= right, nil
		}
	case LargerThan:
		switch param.Field.Type {
		case FieldTypeInt64:
			left, err := f.ToInt64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToInt64()
			if err != nil {
				return false, err
			}
			return left > right, nil
		case FieldTypeFloat64:
			left, err := f.ToFloat64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToFloat64()
			if err != nil {
				return false, err
			}
			return left > right, nil
		default:
			left, err := f.ToString()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToString()
			if err != nil {
				return false, err
			}
			return left > right, nil
		}
	case LargerOrEqualThan:
		switch param.Field.Type {
		case FieldTypeInt64:
			left, err := f.ToInt64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToInt64()
			if err != nil {
				return false, err
			}
			return left >= right, nil
		case FieldTypeFloat64:
			left, err := f.ToFloat64()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToFloat64()
			if err != nil {
				return false, err
			}
			return left >= right, nil
		default:
			left, err := f.ToString()
			if err != nil {
				return false, err
			}
			right, err := param.Field.ToString()
			if err != nil {
				return false, err
			}
			return left > right, nil
		}
	// all other considered as Equal compare:
	default:
		left, err := f.ToValue()
		if err != nil {
			return false, err
		}
		right, err := param.Field.ToValue()
		if err != nil {
			return false, err
		}
		return reflect.DeepEqual(left, right), err
	}
}
