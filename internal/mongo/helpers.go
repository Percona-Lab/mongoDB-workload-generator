package mongo

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

// getTypeFromPlaceholder converts "<int>" -> "int"
func getTypeFromPlaceholder(p string) string {
	if len(p) > 2 && p[0] == '<' && p[len(p)-1] == '>' {
		return p[1 : len(p)-1]
	}
	return ""
}

func cloneSlice(src []interface{}) []interface{} {
	if src == nil {
		return nil
	}
	dst := make([]interface{}, len(src))
	for i, v := range src {
		switch t := v.(type) {
		case map[string]interface{}:
			dst[i] = cloneMap(t)
		case []interface{}:
			dst[i] = cloneSlice(t)
		default:
			dst[i] = t
		}
	}
	return dst
}

// Utility to convert given projection map[string]interface{} into bson.D if needed.
// Many of your query definitions probably already use map[string]interface{}; driver will accept bson.M
func toBsonM(m map[string]interface{}) bson.M {
	if m == nil {
		return nil
	}
	out := bson.M{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
