package btree

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/kkumaki12/minidb/buffer"
	"github.com/kkumaki12/minidb/disk"
)

// テスト用のヘルパー関数
func setupTestEnv(t *testing.T) (*buffer.BufferPoolManager, func()) {
	t.Helper()

	// 一時ファイルを作成
	tmpFile, err := os.CreateTemp("", "btree_test_*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	// DiskManagerを作成
	diskMgr, err := disk.Open(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		t.Fatalf("failed to open disk manager: %v", err)
	}

	// BufferPoolを作成（小さめのサイズでテスト）
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskMgr, pool)

	cleanup := func() {
		os.Remove(tmpPath)
	}

	return bufmgr, cleanup
}

func TestBTreeCreate(t *testing.T) {
	bufmgr, cleanup := setupTestEnv(t)
	defer cleanup()

	tree, err := Create(bufmgr)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	if tree.MetaPageID != 0 {
		t.Errorf("expected meta page id 0, got %d", tree.MetaPageID)
	}
}

func TestBTreeInsertAndSearch(t *testing.T) {
	bufmgr, cleanup := setupTestEnv(t)
	defer cleanup()

	tree, err := Create(bufmgr)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// 挿入
	testData := []struct {
		key   string
		value string
	}{
		{"apple", "りんご"},
		{"banana", "バナナ"},
		{"cherry", "さくらんぼ"},
	}

	for _, d := range testData {
		if err := tree.Insert(bufmgr, []byte(d.key), []byte(d.value)); err != nil {
			t.Fatalf("failed to insert %s: %v", d.key, err)
		}
	}

	// 検索
	for _, d := range testData {
		iter, err := tree.Search(bufmgr, NewSearchKey([]byte(d.key)))
		if err != nil {
			t.Fatalf("failed to search %s: %v", d.key, err)
		}

		pair, err := iter.Next(bufmgr)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if pair == nil {
			t.Fatalf("expected pair for key %s, got nil", d.key)
		}
		if string(pair.Key) != d.key {
			t.Errorf("expected key %s, got %s", d.key, string(pair.Key))
		}
		if string(pair.Value) != d.value {
			t.Errorf("expected value %s, got %s", d.value, string(pair.Value))
		}
	}
}

func TestBTreeDuplicateKey(t *testing.T) {
	bufmgr, cleanup := setupTestEnv(t)
	defer cleanup()

	tree, err := Create(bufmgr)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// 最初の挿入は成功
	if err := tree.Insert(bufmgr, []byte("key"), []byte("value1")); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// 重複キーの挿入はエラー
	err = tree.Insert(bufmgr, []byte("key"), []byte("value2"))
	if err != ErrDuplicateKey {
		t.Errorf("expected ErrDuplicateKey, got %v", err)
	}
}

func TestBTreeIterateAll(t *testing.T) {
	bufmgr, cleanup := setupTestEnv(t)
	defer cleanup()

	tree, err := Create(bufmgr)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// ソート順でないキーを挿入
	keys := []string{"dog", "cat", "ant", "bird", "elephant"}
	for _, k := range keys {
		if err := tree.Insert(bufmgr, []byte(k), []byte(k+"_value")); err != nil {
			t.Fatalf("failed to insert %s: %v", k, err)
		}
	}

	// 先頭から全件取得
	iter, err := tree.Search(bufmgr, NewSearchStart())
	if err != nil {
		t.Fatalf("failed to search from start: %v", err)
	}

	var result []string
	for {
		pair, err := iter.Next(bufmgr)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if pair == nil {
			break
		}
		result = append(result, string(pair.Key))
	}

	// ソート順になっているか確認
	expected := []string{"ant", "bird", "cat", "dog", "elephant"}
	if len(result) != len(expected) {
		t.Fatalf("expected %d results, got %d", len(expected), len(result))
	}
	for i, k := range expected {
		if result[i] != k {
			t.Errorf("expected result[%d]=%s, got %s", i, k, result[i])
		}
	}
}

func TestBTreeManyInserts(t *testing.T) {
	bufmgr, cleanup := setupTestEnv(t)
	defer cleanup()

	tree, err := Create(bufmgr)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// 多数のキーを挿入（分割が発生するはず）
	n := 100
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%05d", i)
		value := fmt.Sprintf("value%05d", i)
		if err := tree.Insert(bufmgr, []byte(key), []byte(value)); err != nil {
			t.Fatalf("failed to insert %s: %v", key, err)
		}
	}

	// 全件検索で確認
	iter, err := tree.Search(bufmgr, NewSearchStart())
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}

	count := 0
	var prevKey []byte
	for {
		pair, err := iter.Next(bufmgr)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if pair == nil {
			break
		}
		// ソート順になっているか
		if prevKey != nil && bytes.Compare(prevKey, pair.Key) >= 0 {
			t.Errorf("keys not sorted: %s >= %s", prevKey, pair.Key)
		}
		prevKey = pair.Key
		count++
	}

	if count != n {
		t.Errorf("expected %d pairs, got %d", n, count)
	}
}

func TestBTreeRangeSearch(t *testing.T) {
	bufmgr, cleanup := setupTestEnv(t)
	defer cleanup()

	tree, err := Create(bufmgr)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// キーを挿入
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%02d", i)
		if err := tree.Insert(bufmgr, []byte(key), []byte("value")); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// key05から検索開始
	iter, err := tree.Search(bufmgr, NewSearchKey([]byte("key05")))
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}

	var result []string
	for {
		pair, err := iter.Next(bufmgr)
		if err != nil {
			t.Fatalf("failed to get next: %v", err)
		}
		if pair == nil {
			break
		}
		result = append(result, string(pair.Key))
	}

	// key05以降が取得できるはず
	expected := []string{"key05", "key06", "key07", "key08", "key09"}
	if len(result) != len(expected) {
		t.Fatalf("expected %d results, got %d: %v", len(expected), len(result), result)
	}
	for i, k := range expected {
		if result[i] != k {
			t.Errorf("expected result[%d]=%s, got %s", i, k, result[i])
		}
	}
}

// ベンチマーク
func BenchmarkBTreeInsert(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "btree_bench_*.db")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	diskMgr, _ := disk.Open(tmpPath)
	pool := buffer.NewBufferPool(100)
	bufmgr := buffer.NewBufferPoolManager(diskMgr, pool)

	tree, _ := Create(bufmgr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		tree.Insert(bufmgr, []byte(key), []byte(value))
	}
}

func BenchmarkBTreeSearch(b *testing.B) {
	tmpFile, _ := os.CreateTemp("", "btree_bench_*.db")
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	diskMgr, _ := disk.Open(tmpPath)
	pool := buffer.NewBufferPool(100)
	bufmgr := buffer.NewBufferPoolManager(diskMgr, pool)

	tree, _ := Create(bufmgr)

	// データを挿入
	n := 10000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%010d", i)
		value := fmt.Sprintf("value%010d", i)
		tree.Insert(bufmgr, []byte(key), []byte(value))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%010d", i%n)
		iter, _ := tree.Search(bufmgr, NewSearchKey([]byte(key)))
		iter.Next(bufmgr)
	}
}
