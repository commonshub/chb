package cmd

import (
	"encoding/json"

	odoosource "github.com/CommonsHub/chb/sources/odoo"
)

func odooDBFromURL(odooURL string) string {
	return odoosource.DBFromURL(odooURL)
}

func odooAuth(odooURL, db, login, password string) (int, error) {
	return odoosource.Auth(odooURL, db, login, password)
}

func odooExec(odooURL, db string, uid int, password, model, method string, args []interface{}, kwargs map[string]interface{}) (json.RawMessage, error) {
	return odoosource.Exec(odooURL, db, uid, password, model, method, args, kwargs)
}
