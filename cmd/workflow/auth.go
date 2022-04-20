package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

var bearerAuths map[string]string

const (
	// AuthorizationHeader is the header key to get the authorization token
	AuthorizationHeader = "authorization"
	tokenPrefix         = "Bearer "
)

func BearerAuthenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "" && r.URL.Path != "/" {
			tokens := []string{r.Header.Get(AuthorizationHeader)}

			if err := authenticate([]string{"default"}, tokens); err != nil {
				w.WriteHeader(http.StatusForbidden)
				json.NewEncoder(w).Encode(err.Error())
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// authenticate returns nil or Error(codes.Unauthenticated)
// return grpc error
func authenticate(tokenKeys []string, tokens []string) error {
	if bearerAuths == nil {
		return fmt.Errorf("fatal error: no auth info found")
	}
	err := fmt.Errorf("token not found")
	for _, tokenKey := range tokenKeys {
		if bearerAuths[tokenKey] == "" {
			return nil // No auth required
		}

		// First valid token
		for _, token := range tokens {
			if token != "" {
				if !strings.HasPrefix(token, tokenPrefix) {
					err = fmt.Errorf(`missing "` + tokenPrefix + `" prefix`)
				} else if strings.TrimPrefix(token, tokenPrefix) != bearerAuths[tokenKey] {
					err = fmt.Errorf("invalid token")
				} else {
					return nil
				}
			}
		}
	}
	return err
}
