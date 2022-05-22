package common

import "kry127.ru/dtextension/go/pkg/api/v0_2"

func OkResult() *v0_2.Result {
	return &v0_2.Result{Ok: true}
}

func ErrorResult(error string) *v0_2.Result {
	return &v0_2.Result{Ok: false, Error: error}
}
