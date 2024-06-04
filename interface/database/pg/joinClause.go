package pg

import (
	"fmt"
	"strings"
)

func limitOffsetClause(page, limit int) string {
	if limit > 0 {
		if page > 0 {
			return fmt.Sprintf(" LIMIT %d OFFSET %d", limit, page*limit)
		}
		return fmt.Sprintf(" LIMIT %d", limit)
	}
	return ""
}

// parseString to be used by LIKE
// * will be replace by %, "?" by "_", "_" by "\\_" and (?i) suffix for case-insensitivity
// Return false if the string does not have ? or *
func parseString(s string) (string, bool) {
	s = strings.ReplaceAll(s, "_", "\\_")
	news := strings.ReplaceAll(strings.ReplaceAll(s, "*", "%"), "?", "_")
	return news, s != news
}

// parse value to be used by LIKE
// * will be replace by %, "?" by "_" and (?i) suffix for case-insensitivity
// Return operator =, LIKE or ILIKE
func parseLike(value string) (string, string) {
	if strings.HasSuffix(value, "(?i)") {
		s, _ := parseString(value[0 : len(value)-4])
		return s, "ILIKE"
	}
	if newv, parsed := parseString(value); parsed {
		return newv, "LIKE"
	}
	return value, "="
}

type joinClause struct {
	Parameters []interface{}
	clause     []string
}

func (wc *joinClause) append(clause string, parameters ...interface{}) {
	positions := []interface{}{}
	for i := range parameters {
		positions = append(positions, len(wc.Parameters)+i+1)
	}

	wc.Parameters = append(wc.Parameters, parameters...)
	wc.clause = append(wc.clause, fmt.Sprintf(clause, positions...))
}

func (wc joinClause) WhereClause() string {
	return wc.Clause(" WHERE ", " AND ", "")
}

func (wc joinClause) Clause(prefix, sep, suffix string) string {
	if len(wc.clause) > 0 {
		return prefix + strings.Join(wc.clause, sep) + suffix
	}
	return ""
}
