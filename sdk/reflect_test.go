package sdk

import "reflect"

func reflectType[T any]() reflect.Type { return reflect.TypeFor[T]() }
