package kafka

import (
	"testing"
)

// Test that Add returns true/false if an eviction occurred
func TestLruAdd(t *testing.T) {
	l := NewSimpleLRU(1, nil)
	l.Add(1, 'a')
	l.Add(2, 'b')

	_, ok := l.Get(1)

	if ok {
		t.Errorf("Key should have been evicted %v",ok)
	}

	_, ok = l.Get(2)

	if !ok {
		t.Errorf("Key should not have been evicted")
	}

}

// Test that Add returns true/false if an eviction occurred
func TestLruPurge(t *testing.T) {
	l := NewSimpleLRU(2, nil)
	l.Add(1, 'a')
	l.Add(2, 'b')

	l.Purge()
	_, ok := l.Get(2)


	if ok {
		t.Errorf("Key should have been purged %v",ok)
	}

	if l.tenants.Len() > 0 {
		t.Errorf("There should be 0 elements in the tenant list")
	}
}