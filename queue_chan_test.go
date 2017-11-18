package yiyidb

import (
	"fmt"
	"time"
	"testing"
)

func TestQueueChan_Enqueue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file,10)
	if err != nil {
		t.Error(err)
	}
	defer q.Close()

	for i := 1; i <= 5; i++ {
		if _, err = q.Enqueue("jac",[]byte(fmt.Sprintf("value for item %d", i))); err != nil {
			t.Error(err)
		}
	}

	for i := 1; i <= 8; i++ {
		if _, err = q.Enqueue("quy",[]byte(fmt.Sprintf("value for item %d", i))); err != nil {
			t.Error(err)
		}
	}

	for i := 1; i <= 5; i++ {
		deqItem, err := q.Dequeue("jac")
		if err != nil {
			t.Error(err)
		}
		fmt.Println("deq:",deqItem.ID, string(deqItem.Value))
	}

	for i := 1; i <= 8; i++ {
		deqItem, err := q.Dequeue("quy")
		if err != nil {
			t.Error(err)
		}
		fmt.Println("deq:",deqItem.ID, string(deqItem.Value))
	}
}

func TestChanQueue_Clear(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file,10)
	if err != nil {
		t.Error(err)
	}
	defer q.Close()
	for i := 1; i <= 5; i++ {
		if _, err = q.Enqueue("jac",[]byte(fmt.Sprintf("value for item %d", i))); err != nil {
			t.Error(err)
		}
	}

	q.Clear("jac")

	_, err = q.Dequeue("jac")
	if err != nil {
		t.Error(err)
	}
	//fmt.Println("deq:",deqItem.ID, string(deqItem.Value))
}

func BenchmarkQueueChan_Dequeue(b *testing.B) {
	// Open test database
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file,3)
	if err != nil {
		b.Error(err)
	}
	defer q.Drop()

	// Fill with dummy data
	for n := 0; n < b.N; n++ {
		if _, err = q.Enqueue("jac",[]byte("value")); err != nil {
			b.Error(err)
		}
	}

	// Start benchmark
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = q.Dequeue("jac")
	}
}