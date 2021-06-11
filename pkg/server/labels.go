package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (ctrl *Controller) labelsHandler(w http.ResponseWriter, _ *http.Request) {
	res := []string{}
	if err := ctrl.s.GetKeys(func(k string) bool {
		res = append(res, k)
		return true
	}); err != nil {
		renderServerError(w, fmt.Sprintf("find keys from storage: %q", err))
		return
	}
	b, err := json.Marshal(res)
	if err != nil {
		renderServerError(w, fmt.Sprintf("json marshal: %q", err))
		return
	}
	w.WriteHeader(200)
	w.Write(b)
}

func (ctrl *Controller) labelValuesHandler(w http.ResponseWriter, r *http.Request) {
	res := []string{}
	labelName := r.URL.Query().Get("label")
	if err := ctrl.s.GetValues(labelName, func(v string) bool {
		res = append(res, v)
		return true
	}); err != nil {
		renderServerError(w, fmt.Sprintf("find values from storage: %q", err))
		return
	}
	b, err := json.Marshal(res)
	if err != nil {
		renderServerError(w, fmt.Sprintf("json marshal: %q", err))
		return
	}
	w.WriteHeader(200)
	w.Write(b)
}
