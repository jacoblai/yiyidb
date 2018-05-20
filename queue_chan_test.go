package yiyidb

import (
	"fmt"
	"time"
	"testing"
	"strconv"
	"github.com/stretchr/testify/assert"
)

func TestChanQueue_EnqueueObject(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file, 10)
	if err != nil {
		t.Error(err)
	}
	defer q.Close()

	type object struct {
		Value []byte
		Key   string
	}

	msg := &object{}
	msg.Key = "dkfjdkf"
	msg.Value = []byte("ddddd")

	for i := 1; i <= 5; i++ {
		msg := object{
			Key:   "dkfjdkf" + strconv.Itoa(i),
			Value: []byte("ddddd" + strconv.Itoa(i)),
		}
		_, err := q.EnqueueObject("jac", msg)
		assert.NoError(t, err)
	}

	vals, err := q.PeekStart("jac")
	assert.NoError(t, err)
	remsg := object{}
	err = vals[0].ToObject(&remsg)
	assert.NoError(t, err)
	assert.Equal(t, remsg.Value, []byte("ddddd1"))

	q.Drop()
}

func TestQueueChan_Enqueue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file, 10)
	if err != nil {
		t.Error(err)
	}
	defer q.Close()

	for i := 1; i <= 5; i++ {
		_, err = q.Enqueue("jac", []byte(fmt.Sprintf("value for item %d", i)))
		assert.NoError(t, err)
	}

	for i := 1; i <= 8; i++ {
		_, err = q.Enqueue("quy", []byte(fmt.Sprintf("value for item %d", i)))
		assert.NoError(t, err)
	}

	for i := 1; i <= 5; i++ {
		_, err := q.Dequeue("jac")
		assert.NoError(t, err)
	}

	for i := 1; i <= 8; i++ {
		_, err := q.Dequeue("quy")
		assert.NoError(t, err)
	}

	q.Drop()
}

func TestChanQueue_Clear(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file, 10)
	if err != nil {
		t.Error(err)
	}
	defer q.Close()
	for i := 1; i <= 5; i++ {
		if _, err = q.Enqueue("jac", []byte(fmt.Sprintf("value for item %d", i))); err != nil {
			t.Error(err)
		}
	}

	q.Clear("jac")

	_, err = q.Dequeue("jac")
	assert.Equal(t, err.Error(), "queue is empty")
	//fmt.Println("deq:",deqItem.ID, string(deqItem.Value))
}

func BenchmarkQueueChan_Dequeue(b *testing.B) {
	// Open test database
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenChanQueue(file, 3)
	if err != nil {
		b.Error(err)
	}
	defer q.Drop()

	// Fill with dummy data
	for n := 0; n < b.N; n++ {
		if _, err = q.Enqueue("jac", []byte("value")); err != nil {
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
