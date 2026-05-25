package redis

import "testing"

func TestCompareWorkerIDsUsesNaturalNumericOrder(t *testing.T) {
	if got := compareWorkerIDs("driver_1", "driver_10"); got >= 0 {
		t.Fatalf("expected driver_1 to sort before driver_10, got %d", got)
	}
	if got := compareWorkerIDs("driver_10", "driver_100"); got >= 0 {
		t.Fatalf("expected driver_10 to sort before driver_100, got %d", got)
	}
	if got := compareWorkerIDs("driver_100", "driver_10"); got <= 0 {
		t.Fatalf("expected driver_100 to sort after driver_10, got %d", got)
	}
}
