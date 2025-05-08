package util

func PackCoordinates(lat, lon float64) uint64 {

	scaledLat := uint64((lat * 1e7) + (90 * 1e7))
	scaledLon := uint64((lon * 1e7) + (180 * 1e7))

	lonPart := (scaledLon & 0xFFFFFFFF) << 31
	latPart := scaledLat & 0x7FFFFFFF

	return lonPart | latPart
}

func UnpackCoordinates(packed uint64) (lat, lon float64) {
	lat = (float64(packed&0x7FFFFFFF) - (90 * 1e7)) / 1e7
	lon = (float64((packed>>31)&0xFFFFFFFF) - (180 * 1e7)) / 1e7
	return lat, lon
}
