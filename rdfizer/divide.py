import csv

with open('1-csv/shapes-trips.csv', 'r') as f_shapes_trips:
	with open('1-csv/shapes-trips-1.csv', 'w') as f_1:
		with open('1-csv/shapes-trips-2.csv', 'w') as f_2:
				shapes_trips = csv.DictReader(f_shapes_trips)
				w_1 = csv.writer(f_1)
				w_2 = csv.writer(f_2)
				headers = ["route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name", "direction_id",
					"block_id", "shape_id", "wheelchair_accessible", "shape_pt_lat", "shape_pt_lon", 
					 "shape_pt_sequence", "shape_dist"]
				w_1.writerow(headers)
				w_2.writerow(headers)
				i = 0
				for st in shapes_trips:
					line = []
					if i < 119541:
						for elements in st:
							line.append(st[elements])
						w_1.writerow(line)
					else:
						for elements in st:
							line.append(st[elements])
						w_2.writerow(line)
					i += 1
