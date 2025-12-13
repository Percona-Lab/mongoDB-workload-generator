package workloads

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/Percona-Lab/mongoDB-workload-generator/internal/config"
	"github.com/Percona-Lab/mongoDB-workload-generator/internal/datagen"
	"github.com/brianvoe/gofakeit/v6"
)

// simplified but realistic plane types & amenities
var planeTypes = []string{"Boeing 737", "Airbus A320", "Embraer E190", "Bombardier CRJ900", "Boeing 777", "Airbus A350"}
var amenities = []string{"WiFi", "TV", "Power outlets", "Hot meals", "Priority boarding", "Extra legroom"}

const minTotalSeats = 10
const maxTotalSeats = 50

// randomEquipment produces an equipment object
func randomEquipment(rng *rand.Rand) map[string]interface{} {
	totalSeats := rng.Intn(maxTotalSeats-minTotalSeats+1) + minTotalSeats

	numAmenities := rng.Intn(4) + 2
	perm := rng.Perm(len(amenities))
	picked := make([]string, 0, numAmenities)
	for i := 0; i < numAmenities; i++ {
		picked = append(picked, amenities[perm[i]])
	}
	return map[string]interface{}{
		"plane_type":  planeTypes[perm[0]%len(planeTypes)],
		"total_seats": totalSeats,
		"amenities":   picked,
	}
}

// randomPassengers creates a list of passengers with UNIQUE seat assignments
func randomPassengers(totalSeats int, seatsAvailable int, rng *rand.Rand) []map[string]interface{} {
	faker := gofakeit.New(rng.Int63())
	numPassengers := totalSeats - seatsAvailable
	if numPassengers < 1 {
		numPassengers = 1
	}
	// Safety check: ensure we don't try to create more passengers than seats
	if numPassengers > totalSeats {
		numPassengers = totalSeats
	}

	passengers := make([]map[string]interface{}, numPassengers)
	seatLetters := []string{"A", "B", "C", "D", "E", "F"}

	// 1. Create a "deck" of all available seats on the plane
	//    We fill rows sequentially (1A, 1B... 2A, 2B...) until we hit totalSeats
	allSeats := make([]string, 0, totalSeats)
	currentRow := 1
	for len(allSeats) < totalSeats {
		for _, letter := range seatLetters {
			if len(allSeats) >= totalSeats {
				break
			}
			allSeats = append(allSeats, fmt.Sprintf("%d%s", currentRow, letter))
		}
		currentRow++
	}

	// 2. Shuffle the deck to ensure random assignment
	rng.Shuffle(len(allSeats), func(i, j int) {
		allSeats[i], allSeats[j] = allSeats[j], allSeats[i]
	})

	for i := 0; i < numPassengers; i++ {
		n := rng.Intn(99999999) + 1
		ticket := fmt.Sprintf("TCK-%08d", n)

		passengers[i] = map[string]interface{}{
			"passenger_id":  i + 1,
			"name":          fmt.Sprintf("%s %s", faker.FirstName(), faker.LastName()),
			"ticket_number": ticket,
			// 3. Assign a unique seat from the shuffled deck
			"seat_number": allSeats[i],
		}
	}
	return passengers
}

// GenerateDefaultDocument produces a document using the collection def if provided.
func GenerateDefaultDocument(col config.CollectionDefinition) map[string]interface{} {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	faker := gofakeit.New(rng.Int63()) // Create ONCE
	doc := make(map[string]interface{})

	if len(col.Fields) > 0 {
		var equipmentData map[string]interface{}
		var totalSeats int
		var seatsAvailable int

		for fname, fdef := range col.Fields {
			// 1. Check for Domain-Specific Providers first
			if fdef.Provider == "flight_code" {
				doc[fname] = fmt.Sprintf("%s%d", strings.ToUpper(faker.LetterN(2)), faker.Number(100, 999))
				continue
			}
			if fdef.Provider == "gate" {
				letter := rune('A' + rng.Intn(6))
				number := rng.Intn(50) + 1
				doc[fname] = fmt.Sprintf("%c%d", letter, number)
				continue
			}

			// 2. Handle Complex Flight Fields
			switch fname {
			case "equipment":
				equipmentData = randomEquipment(rng)
				doc[fname] = equipmentData
				if ts, ok := equipmentData["total_seats"].(int); ok {
					totalSeats = ts
				}
			case "seats_available":
				seatsAvailable = rng.Intn(maxTotalSeats) + 1
				doc[fname] = seatsAvailable
			case "passengers":
				continue // handled at the end
			default:
				// 3. Fallback to Generic Generator using EXISTING faker
				doc[fname] = datagen.RandomValueWithFaker(fdef, faker)
			}
		}

		// --- LOGICAL CONSISTENCY CHECKS ---

		// 1. Fix Origin == Destination collision
		if origin, ok := doc["origin"].(string); ok {
			if dest, ok := doc["destination"].(string); ok {
				// While they are the same, pick a new destination
				for origin == dest {
					dest = faker.City()
				}
				doc["destination"] = dest
			}
		}

		// 2. Ensure seats
		if totalSeats == 0 {
			totalSeats = maxTotalSeats
		}
		if seatsAvailable > totalSeats {
			seatsAvailable = rng.Intn(totalSeats) + 1
			doc["seats_available"] = seatsAvailable
		}

		// 3. Fill passengers
		if _, ok := col.Fields["passengers"]; ok {
			doc["passengers"] = randomPassengers(totalSeats, seatsAvailable, rng)
		}
		return doc
	}

	// Fallback if no schema is provided
	doc["flight_id"] = rng.Intn(10000)
	doc["origin"] = faker.City()

	// Ensure distinct fallback cities
	dest := faker.City()
	for dest == doc["origin"] {
		dest = faker.City()
	}
	doc["destination"] = dest

	doc["duration_minutes"] = rng.Intn(400)
	doc["seats_available"] = rng.Intn(300)
	doc["equipment"] = map[string]interface{}{
		"plane_type": fmt.Sprintf("A%d", rng.Intn(320)),
	}
	return doc
}

// GenerateDefaultUpdate returns a MongoDB update document specific to the flights workload.
func GenerateDefaultUpdate(rng *rand.Rand) map[string]interface{} {
	if rng.Intn(2) == 0 {
		return map[string]interface{}{
			"$inc": map[string]interface{}{"seats_available": rng.Intn(5) + 1},
		}
	}
	return map[string]interface{}{
		"$set": map[string]interface{}{"duration_minutes": rng.Intn(400) + 30},
	}
}
