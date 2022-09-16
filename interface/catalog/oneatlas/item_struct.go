package oneatlas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

type catalogRequestParameter struct {
	Constellation   string    `json:"constellation,omitempty"`
	ItemsPerPage    int       `json:"itemsPerPage,omitempty"`
	StartPage       int       `json:"startPage,omitempty"`
	ProcessingLevel string    `json:"processingLevel,omitempty"`
	ProductType     string    `json:"productType,omitempty"`
	SortBy          string    `json:"sortBy,omitempty"`
	AcquisitionDate string    `json:"acquisitionDate,omitempty"`
	Platform        string    `json:"platform,omitempty"`
	CloudCover      string    `json:"cloudCover,omitempty"`
	IncidenceAngle  string    `json:"incidenceAngle,omitempty"`
	Workspace       string    `json:"workspace,omitempty"`
	Relation        string    `json:"relation,omitempty"`
	Geometry        *geometry `json:"geometry,omitempty"`
}

type catalogResponse struct {
	Error        bool   `json:"error"`
	ItemsPerPage int    `json:"itemsPerPage"`
	StartIndex   int    `json:"startIndex"`
	TotalResults int    `json:"totalResults"`
	Type         string `json:"type"`
	Features     []struct {
		Links      *links                 `json:"_links"`
		Geometry   *geometry              `json:"geometry"`
		Properties map[string]interface{} `json:"properties"`
		Type       string                 `json:"type"`
	} `json:"features"`
}

type geometry struct {
	Coordinates [][][2]float64 `json:"coordinates"`
	Type        string         `json:"type"`
}

type links map[string][]link

type link struct {
	Href       string `json:"href"`
	Method     string `json:"method,omitempty"`
	Name       string `json:"name,omitempty"`
	Type       string `json:"type,omitempty"`
	ResourceId string `json:"resourceId,omitempty"`
	InSlice    bool   `json:"-"`
}

func (l *links) GetLinks(key string) []link {
	if l == nil {
		return []link{}
	}

	v, ok := (*l)[key]
	if !ok || len(v) == 0 {
		return []link{}
	}
	return v
}

func (l *links) UnmarshalJSON(b []byte) error {
	var lis map[string]json.RawMessage
	err := json.Unmarshal(b, &lis)
	if err != nil {
		return err
	}

	*l = make(links)
	for k, v := range lis {
		li := link{}
		if err := json.Unmarshal(v, &li); err == nil {
			(*l)[k] = []link{li}
			continue
		}

		var array []link
		if err := json.Unmarshal(v, &array); err == nil {
			if len(array) == 1 {
				array[0].InSlice = true
			}
			(*l)[k] = array
			continue
		}

		return err
	}

	return nil
}

func (l links) MarshalJSON() ([]byte, error) {
	if l == nil {
		return []byte("null"), nil
	}
	buffer := bytes.NewBufferString("{")
	length := len(l)
	for _, k := range sortedKeys(l) {
		var jsonValue []byte
		var err error
		switch len(l[k]) {
		case 0:
			jsonValue = append(jsonValue, "null"...)
		case 1:
			if l[k][0].InSlice {
				jsonValue, err = json.Marshal(l[k])
			} else {
				jsonValue, err = json.Marshal(l[k][0])
			}
		default:
			sort.Slice(l[k], func(i int, j int) bool {
				return l[k][i].Type < l[k][j].Type
			})
			jsonValue, err = json.Marshal(l[k])
		}
		if err != nil {
			return nil, err
		}

		buffer.WriteString(fmt.Sprintf(`"%v":%s`, k, string(jsonValue)))
		length--
		if length > 0 {
			buffer.WriteRune(',')
		}
	}
	buffer.WriteRune('}')
	return buffer.Bytes(), nil
}

func sortedKeys(m links) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	return keys
}
