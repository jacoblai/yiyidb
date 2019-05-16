package yiyidb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestQueueClose(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	_, err = q.EnqueueString("value")
	assert.NoError(t, err)

	assert.Equal(t, int(q.Length()), 1)

	q.Close()

	_, err = q.Dequeue()
	assert.Equal(t, err, ErrDBClosed)

	assert.Equal(t, int(q.Length()), 0)

	q.Drop()
}

func TestQueueDrop(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}

	if _, err = os.Stat(file); os.IsNotExist(err) {
		t.Error(err)
	}

	q.Drop()

	if _, err = os.Stat(file); err == nil {
		t.Error("Expected directory for test database to have been deleted")
	}
}

func TestQueueEnqueue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	assert.Equal(t, int(q.Length()), 10)

	q.Drop()
}

func TestQueueDequeue(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	assert.Equal(t, int(q.Length()), 10)

	deqItem, err := q.Dequeue()
	assert.NoError(t, err)

	assert.Equal(t, int(q.Length()), 9)

	compStr := "value for item 1"

	if deqItem.ToString() != compStr {
		t.Errorf("Expected string to be '%s', got '%s'", compStr, deqItem.ToString())
	}

	//test head = tail
	for i := 1; i <= 9; i++ {
		_, err := q.Dequeue()
		assert.NoError(t, err)
	}

	assert.Equal(t, int(q.Length()), 0)

	_, err = q.EnqueueString(fmt.Sprintf("value for item %d", 888))
	assert.NoError(t, err)

	deqItem, err = q.Dequeue()
	assert.NoError(t, err)

	compStr = "value for item 888"
	assert.Equal(t, deqItem.ToString(), compStr)

	q.Drop()
}

func TestQueuePeek(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	compStr := "value for item"

	_, err = q.EnqueueString(compStr)
	assert.NoError(t, err)

	peekItem, err := q.Peek()
	assert.NoError(t, err)

	assert.Equal(t, peekItem.ToString(), compStr)

	assert.Equal(t, int(q.Length()), 1)

	q.Drop()
}

func TestQueuePeekByOffset(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	compStrFirst := "value for item 1"
	compStrLast := "value for item 10"
	compStr := "value for item 4"

	peekFirstItem, err := q.PeekByOffset(0)
	assert.NoError(t, err)

	assert.Equal(t, peekFirstItem.ToString(), compStrFirst)

	peekLastItem, err := q.PeekByOffset(9)
	assert.NoError(t, err)

	assert.Equal(t, peekLastItem.ToString(), compStrLast)

	peekItem, err := q.PeekByOffset(3)
	assert.NoError(t, err)

	assert.Equal(t, peekItem.ToString(), compStr)
	assert.Equal(t, int(q.Length()), 10)

	q.Drop()
}

func TestQueuePeekByID(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	compStr := "value for item 3"

	peekItem, err := q.PeekByID(3)
	assert.NoError(t, err)

	assert.Equal(t, peekItem.ToString(), compStr)

	assert.Equal(t, int(q.Length()), 10)

	q.Drop()
}

func TestQueueUpdate(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	item, err := q.PeekByID(3)
	assert.NoError(t, err)

	oldCompStr := "value for item 3"
	newCompStr := "new value for item 3"
	assert.Equal(t, item.ToString(), oldCompStr)

	updatedItem, err := q.Update(item.ID, []byte(newCompStr))
	assert.NoError(t, err)

	assert.Equal(t, updatedItem.ToString(), newCompStr)

	newItem, err := q.PeekByID(3)
	assert.NoError(t, err)

	assert.Equal(t, newItem.ToString(), newCompStr)

	q.Drop()
}

func TestQueueUpdateString(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	item, err := q.PeekByID(3)
	assert.NoError(t, err)

	oldCompStr := "value for item 3"
	newCompStr := "new value for item 3"

	assert.Equal(t, item.ToString(), oldCompStr)

	updatedItem, err := q.UpdateString(item.ID, newCompStr)
	assert.NoError(t, err)

	assert.Equal(t, updatedItem.ToString(), newCompStr)

	newItem, err := q.PeekByID(3)
	assert.NoError(t, err)

	assert.Equal(t, newItem.ToString(), newCompStr)

	q.Drop()
}

func TestQueueUpdateObject(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	type object struct {
		Value int
	}

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueObject(object{i})
		assert.NoError(t, err)
	}

	item, err := q.PeekByID(3)
	assert.NoError(t, err)

	oldCompObj := object{3}
	newCompObj := object{33}

	var obj object
	err = item.ToObject(&obj)
	assert.NoError(t, err)

	assert.Equal(t, obj, oldCompObj)

	updatedItem, err := q.UpdateObject(item.ID, newCompObj)
	assert.NoError(t, err)

	err = updatedItem.ToObject(&obj)
	assert.NoError(t, err)

	assert.Equal(t, obj, newCompObj)

	newItem, err := q.PeekByID(3)
	assert.NoError(t, err)

	err = newItem.ToObject(&obj)
	assert.NoError(t, err)

	assert.Equal(t, obj, newCompObj)

	q.Drop()
}

func TestQueueUpdateOutOfBounds(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	for i := 1; i <= 10; i++ {
		_, err = q.EnqueueString(fmt.Sprintf("value for item %d", i))
		assert.NoError(t, err)
	}

	assert.Equal(t, int(q.Length()), 10)

	deqItem, err := q.Dequeue()
	assert.NoError(t, err)

	assert.Equal(t, int(q.Length()), 9)

	_, err = q.Update(deqItem.ID, []byte(`new value`))
	assert.Equal(t, err, ErrOutOfBounds)

	_, err = q.Update(deqItem.ID+1, []byte(`new value`))
	assert.NoError(t, err)

	q.Drop()
}

func TestQueueEmpty(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	_, err = q.EnqueueString("value for item")
	assert.NoError(t, err)

	_, err = q.Dequeue()
	assert.NoError(t, err)

	_, err = q.Dequeue()
	assert.Equal(t, err, ErrEmpty)

	q.Drop()
}

func TestQueueOutOfBounds(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	_, err = q.EnqueueString("value for item")
	assert.NoError(t, err)

	_, err = q.PeekByOffset(2)
	assert.Equal(t, err, ErrOutOfBounds)

	q.Drop()
}

func TestQueue_EnqueueBat(t *testing.T) {
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		t.Error(err)
	}
	defer q.Drop()

	vals := make([][]byte, 0)
	for i := 1; i < 500; i++ {
		vals = append(vals, []byte("test values"+strconv.Itoa(i)))
	}

	err = q.EnqueueBatch(vals)
	assert.NoError(t, err)

	item, err := q.PeekByID(499)
	assert.NoError(t, err)
	assert.Equal(t, item.Value, []byte("test values499"))

	q.Drop()
}

func BenchmarkQueueEnqueue(b *testing.B) {
	// Open test database
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		b.Error(err)
	}
	defer q.Drop()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, _ = q.Enqueue([]byte("value"))
	}
}

type object struct {
	aaa int
}

func BenchmarkQueueDequeue(b *testing.B) {
	// Open test database
	file := fmt.Sprintf("test_db_%d", time.Now().UnixNano())
	q, err := OpenQueue(file)
	if err != nil {
		b.Error(err)
	}
	defer q.Drop()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err = q.EnqueueObject(object{2}); err != nil {
				b.Error(err)
			}
		}
	})

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if item, err := q.Dequeue(); err != nil {
				b.Error(err)
			} else {
				var obj object
				item.ToObject(&obj)
				b.Log(obj.aaa)
			}
		}
	})
}
