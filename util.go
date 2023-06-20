package memodb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Fill struct with fieldvalues
func FillFieldValues(source map[string]FieldValue, dest interface{}) (err error) {
	defer recoverPanic(&err)

	destType := reflect.TypeOf(dest)
	if destType.Kind() != reflect.Pointer {
		return ErrNotImplemented
	}

	valDest := reflect.ValueOf(dest).Elem()
	for k, v := range source {

		defer recoverPanic(&err)

		keySplit := strings.Split(k, `/`)
		if len(keySplit) >= 1 {
			field := valDest.FieldByName(keySplit[1])
			if !field.IsValid() && !field.CanSet() {
				err = errors.New("field does not exists bro")
				return
			}

			switch v.Type {
			case FieldTypeInt64:
				defer recoverPanic(&err)
				field.SetInt(v.Value.(int64))
			case FieldTypeFloat64:
				defer recoverPanic(&err)
				field.SetFloat(v.Value.(float64))
			case FieldTypeString:
				defer recoverPanic(&err)
				field.SetString(v.Value.(string))
			default:
				continue
			}

		}
	}

	return nil
}

func ToFieldValues(data interface{}, label string, dest map[string]FieldValue) (err error) {
	//cover with trycatch since it is threading on unsafe territory
	defer recoverPanic(&err)

	dataType := reflect.TypeOf(data)
	switch dataType.Kind() {
	case reflect.Struct:
		defer recoverPanic(&err)

		// move struct content according to tags, current implementation only one level supported
		val := reflect.ValueOf(data)
		errPool := make(chan error, val.NumField())
		for i := 0; i < val.NumField(); i++ {
			sfield := reflect.Indirect(val).Field(i)
			mtag := val.Type().Field(i).Tag.Get(`memodb`)
			switch mtag {
			case MemodbTagInt:
				toConvert := sfield.Int()
				errPool <- ToFieldValues(toConvert, label+`/`+val.Type().Field(i).Name, dest)
			case MemodbTagFloat:
				toConvert := sfield.Float()
				errPool <- ToFieldValues(toConvert, label+`/`+val.Type().Field(i).Name, dest)
			case MemodbTagString:
				toConvert := sfield.String()
				errPool <- ToFieldValues(toConvert, label+`/`+val.Type().Field(i).Name, dest)
			default:
				errPool <- nil
			}
		}
		errs := <-errPool
		if errs != nil {
			return err
		}

	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
		defer recoverPanic(&err)

		val := data.(int64)
		dest[label] = FieldValue{
			Label: label,
			Value: val,
			Type:  FieldTypeInt64,
		}
	case reflect.Float64, reflect.Float32:
		defer recoverPanic(&err)

		val := data.(float64)
		dest[label] = FieldValue{
			Label: label,
			Value: val,
			Type:  FieldTypeFloat64,
		}
	case reflect.String:
		defer recoverPanic(&err)

		val := data.(string)
		dest[label] = FieldValue{
			Label: label,
			Value: val,
			Type:  FieldTypeString,
		}
	}

	return nil
}

func GenerateMemoStructure(source interface{}) (memost MemoStructure, err error) {
	defer recoverPanic(&err)

	sourcetType := reflect.TypeOf(source)
	if sourcetType.Kind() != reflect.Struct {
		return memost, ErrNotImplemented
	}

	val := reflect.ValueOf(source)
	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		memosttag := field.Tag.Get(`memost`)
		dbTag := field.Tag.Get(`memodb`)

		included := false
		currentField := Field{
			FieldType: toFieldType(dbTag),
			Label:     field.Name,
		}

		switch memosttag {
		case MemoStructureKey:
			memost.KeyField = currentField
			included = true
		case MemoStructureIndex:
			memost.IndexedFields = append(memost.IndexedFields, currentField)
			included = true
		}

		//included field without tags are considered field
		if ((dbTag != `-` && dbTag != ``) || memosttag != ``) || included {
			memost.Fields = append(memost.Fields, currentField)
		}
	}

	return
}

func recoverPanic(err *error) {
	if pnc := recover(); pnc != nil {
		*err = errors.New("error processing to field:" + fmt.Sprintf("%+v", pnc))
	}
}

func toFieldType(str string) FieldType {
	switch str {
	case MemodbTagInt:
		return FieldTypeInt64
	case MemodbTagFloat:
		return FieldTypeFloat64
	case MemodbTagString:
		return FieldTypeString
	default:
		return 0
	}

}
