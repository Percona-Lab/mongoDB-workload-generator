from faker.providers import BaseProvider # type: ignore
import random
import string
from datetime import datetime, timedelta

class CustomProvider(BaseProvider):
    aircraft_seat_map = {
        "Airbus A320": 170,
        "Boeing 737": 160,
        "CRJ 1000": 100,
        "Embraer E190": 90,
        "Dash 8-400": 85,
        "ATR-72": 75,
        "ERJ-145": 50
    }

    # Define the list of states once as a class attribute
    us_states = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
    ]

    def plane_type(self):
        return random.choice([
            "Airbus A320", "Boeing 737", "CRJ 1000", "Embraer E190", "Dash 8-400", "ATR-72", "ERJ-145"
        ])

    def state_abbr(self):
        return random.choice(self.us_states)

    def flight_id(self):
        # Generates a random flight_id (integer)
        return random.randint(0, 9999999)

    def random_string(self, length=5):
        # Helper method to create random uppercase string
        letters = string.ascii_uppercase
        return ''.join(random.choice(letters) for _ in range(length))

    def gate(self):
        # Gate as a letter + number 1-10
        return random.choice(string.ascii_uppercase) + str(random.randint(1, 10))
    
    def ticket_number(self):
        # PNR-like ticket: 10 uppercase letters/digits (e.g., M8Y3KQ)
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    def aircraft_and_seats(self):
        # Select plane_type and calculate seats_available and num_passengers
        plane_type = random.choice(list(self.aircraft_seat_map.keys()))
        total_seats = self.aircraft_seat_map[plane_type]
        num_passengers = random.randint(1, min(70, total_seats))
        seats_available = total_seats - num_passengers
        return plane_type, total_seats, num_passengers, seats_available

    def passengers(self, total_seats, num_passengers, fake):
        # Generate passenger list
        seat_letters = ["A","B","C","D","E","F"]
        passengers_list = []
        for idx in range(1, num_passengers + 1):
            seat_number = str(random.randint(1, total_seats // 3)) + random.choice(seat_letters)
            passengers_list.append({
                "passenger_id": idx,
                "name": fake.name(),
                "seat_number": seat_number,
                "ticket_number": self.ticket_number()
            })
        return passengers_list

    def equip(self, plane_type, total_seats):
        return {
            "plane_type": plane_type,
            "total_seats": total_seats,
            "amenities": ["WiFi", "TV", "Power outlets"]
        }

    def flight_code(self):
        return "FLT-" + str(random.randint(100, 999))

    def car_type(self):
        return random.choice([
            "Compact", "Sedan", "SUV", "Convertible", "Pickup", "Minivan", "Luxury"
        ])

    def license_plate(self):
        letters = ''.join(random.choices(string.ascii_uppercase, k=3))
        numbers = ''.join(random.choices(string.digits, k=4))
        return f"{letters}-{numbers}"

    def rental_options(self):
        return {
            "gps": random.choice([True, False]),
            "child_seat": random.choice([True, False]),
            "extra_driver": random.choice([True, False]),
            "insurance": random.choice(["basic", "standard", "premium"])
        }
    
    def rental_info(self):
        rental_date = self.generator.date_time_between(start_date='-30d', end_date='now')
        return_date = rental_date + timedelta(days=random.randint(1, 14))
        pickup_location = self.generator.city()
        drop_off_location = self.generator.city()

        return {
            "rental_date": rental_date,
            "return_date": return_date,
            "pickup_location": pickup_location,
            "drop_off_location": drop_off_location
        }

    def drivers(self):
        num_drivers = random.randint(1, 2)
        return [
            {
                "driver_id": i,
                "name": self.generator.name(),
                "age": random.randint(21, 70),
                "license_number": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
                "license_state": self.state_abbr()
            }
            for i in range(1, num_drivers + 1)
        ]