package datagen

import (
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/Percona-Lab/mongoDB-workload-generator/internal/config"
	"github.com/brianvoe/gofakeit/v6"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// toCamelCase converts snake_case (e.g. "first_name") to CamelCase (e.g. "FirstName")
func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i := range parts {
		if len(parts[i]) > 0 {
			r := []rune(parts[i])
			r[0] = unicode.ToUpper(r[0])
			parts[i] = string(r)
		}
	}
	return strings.Join(parts, "")
}

// RandomValueWithFaker uses an existing Faker instance to generate values.
// This is much faster than creating a new Faker for every field.
func RandomValueWithFaker(def config.CollectionField, faker *gofakeit.Faker) interface{} {
	// Use the RNG inside the faker instance for raw math operations
	rng := faker.Rand

	// 1. Dynamic Provider Lookup (Reflection)
	if def.Provider != "" {
		methodName := toCamelCase(def.Provider)
		fakerVal := reflect.ValueOf(faker)
		method := fakerVal.MethodByName(methodName)

		if method.IsValid() {
			results := method.Call(nil)
			if len(results) > 0 {
				return results[0].Interface()
			}
		}

		// Fallback for special providers
		switch strings.ToLower(def.Provider) {
		case "uuid":
			return faker.UUID()
		case "ssn":
			return faker.SSN()
		}
	}

	// 2. Handle All MongoDB Data Types
	switch strings.ToLower(def.Type) {
	// --- Numbers ---
	case "int", "integer", "int32":
		min := 0
		max := 2147483647
		if def.Min != nil {
			min = *def.Min
		}
		if def.Max != nil {
			max = *def.Max
		}
		return int32(rng.Intn(max-min+1) + min)

	case "long", "int64":
		return rng.Int63()

	case "double", "float":
		min := 0.0
		max := 1000.0
		if def.Min != nil {
			min = float64(*def.Min)
		}
		if def.Max != nil {
			max = float64(*def.Max)
		}
		return min + rng.Float64()*(max-min)

	case "decimal", "decimal128":
		val := fmt.Sprintf("%d.%d", rng.Intn(1000), rng.Intn(100))
		d, _ := bson.ParseDecimal128(val)
		return d

	// --- Strings & Boolean ---
	case "string":
		if def.Provider == "" {
			return fmt.Sprintf("str-%d", rng.Intn(100000))
		}
		return "val" // Should be handled by provider logic

	case "bool", "boolean":
		return rng.Intn(2) == 0

	// --- Dates & Times ---
	case "date", "datetime":
		return time.Now().Add(-time.Duration(rng.Intn(365*24)) * time.Hour)
	case "timestamp":
		return bson.Timestamp{T: uint32(time.Now().Unix()), I: uint32(rng.Intn(100))}

	// --- Identifiers ---
	case "objectid":
		return bson.NewObjectID()

	// --- Complex Structures (Recursion uses the SAME faker instance) ---
	case "object", "document":
		if len(def.Fields) > 0 {
			doc := make(bson.D, 0, len(def.Fields))
			for key, fieldDef := range def.Fields {
				val := RandomValueWithFaker(fieldDef, faker)
				doc = append(doc, bson.E{Key: key, Value: val})
			}
			return doc
		}
		return bson.D{{Key: "nested_random", Value: rng.Intn(100)}}

	case "array":
		size := def.ArraySize
		if size <= 0 {
			size = rng.Intn(5) + 1
		}
		arr := make(bson.A, size)

		if def.Items != nil {
			for i := 0; i < size; i++ {
				arr[i] = RandomValueWithFaker(*def.Items, faker)
			}
		} else {
			for i := 0; i < size; i++ {
				arr[i] = rng.Intn(1000)
			}
		}
		return arr

	default:
		return fmt.Sprintf("unknown-%s", def.Type)
	}
}

// RandomValue convenience wrapper (slower, creates new faker)
func RandomValue(def config.CollectionField) interface{} {
	faker := gofakeit.New(time.Now().UnixNano())
	return RandomValueWithFaker(def, faker)
}
