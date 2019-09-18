package rpc

import (
	"reflect"
	"strings"

	enLocale "github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/videocoin/cloud-api/rpc"
	validator "gopkg.in/go-playground/validator.v9"
	enTrans "gopkg.in/go-playground/validator.v9/translations/en"
)

type requestValidator struct {
	validator  *validator.Validate
	translator *ut.Translator
}

func newRequestValidator() *requestValidator {
	lt := enLocale.New()
	en := &lt

	uniTranslator := ut.New(*en, *en)
	uniEn, _ := uniTranslator.GetTranslator("en")
	translator := &uniEn

	validate := validator.New()
	enTrans.RegisterDefaultTranslations(validate, *translator)

	return &requestValidator{
		validator:  validate,
		translator: translator,
	}

}

func (rv *requestValidator) validate(r interface{}) *rpc.MultiValidationError {
	trans := *rv.translator
	verrs := &rpc.MultiValidationError{}

	serr := rv.validator.Struct(r)
	if serr != nil {
		verrs.Errors = []*rpc.ValidationError{}

		for _, err := range serr.(validator.ValidationErrors) {
			field, _ := reflect.TypeOf(r).Elem().FieldByName(err.Field())
			jsonField := extractValueFromTag(field.Tag.Get("json"))
			verr := &rpc.ValidationError{
				Field:   jsonField,
				Message: err.Translate(trans),
			}
			verrs.Errors = append(verrs.Errors, verr)
		}

		return verrs
	}

	return nil
}

func extractValueFromTag(tag string) string {
	values := strings.Split(tag, ",")
	return values[0]
}
