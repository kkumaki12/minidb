package buffer

import (
	"errors"

	"github.com/kkumaki12/minidb/disk"
)

// エラー定義
var (
	ErrNoFreeBuffer = errors.New("no free buffer available in pool")
)

// Page はページサイズ分のバイト配列
type Page [disk.PageSize]byte

// BufferID はバッファプール内のフレームを識別するインデックス
type BufferID uint64

// Buffer はメモリ上にキャッシュされたページを表す
type Buffer struct {
	PageID   disk.PageID // このバッファが保持しているページのID
	Page     Page        // ページデータ本体
	IsDirty  bool        // ディスクに書き戻す必要があるか
	refCount int         // 参照カウント（0なら evict 可能）
	isValid  bool        // このバッファが有効なページを保持しているか
}

// Frame はバッファプール内の1スロットを表す
// Clock-sweepアルゴリズムで使用するUsageCountを持つ
type Frame struct {
	UsageCount uint64  // 使用カウント（Clock-sweepで使用）
	Buffer     *Buffer // バッファへのポインタ
}

// BufferPool はページをメモリ上にキャッシュするためのプール
type BufferPool struct {
	frames       []Frame  // フレームの配列
	nextVictimID BufferID // 次に置換候補として検査するフレームID（Clock-sweep用）
}

// NewBufferPool は指定サイズのバッファプールを作成する
func NewBufferPool(poolSize int) *BufferPool {
	frames := make([]Frame, poolSize)
	for i := range frames {
		frames[i] = Frame{
			UsageCount: 0,
			Buffer:     &Buffer{},
		}
	}
	return &BufferPool{
		frames:       frames,
		nextVictimID: 0,
	}
}

// Size はプールのサイズを返す
func (p *BufferPool) Size() int {
	return len(p.frames)
}

// Evict はClock-sweepアルゴリズムで置換対象のバッファIDを返す
// 全てのバッファがピンされている場合はエラーを返す
func (p *BufferPool) Evict() (BufferID, error) {
	poolSize := p.Size()
	consecutivePinned := 0

	for {
		nextVictimID := p.nextVictimID
		frame := &p.frames[nextVictimID]

		// UsageCountが0なら、このフレームを置換対象とする
		if frame.UsageCount == 0 {
			return nextVictimID, nil
		}

		// 参照カウントが0（誰も使っていない）ならUsageCountを減らす
		if frame.Buffer.refCount == 0 {
			frame.UsageCount--
			consecutivePinned = 0
		} else {
			// ピンされている（使用中）
			consecutivePinned++
			if consecutivePinned >= poolSize {
				// 全てのバッファがピンされている
				return 0, ErrNoFreeBuffer
			}
		}

		p.nextVictimID = p.incrementID(p.nextVictimID)
	}
}

// incrementID はバッファIDを循環的にインクリメントする
func (p *BufferPool) incrementID(bufferID BufferID) BufferID {
	return BufferID((int(bufferID) + 1) % p.Size())
}

// BufferPoolManager はバッファプールとディスクマネージャを管理する
type BufferPoolManager struct {
	disk      *disk.DiskManager
	pool      *BufferPool
	pageTable map[disk.PageID]BufferID // ページIDからバッファIDへのマッピング
}

// NewBufferPoolManager は新しいBufferPoolManagerを作成する
func NewBufferPoolManager(diskManager *disk.DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk:      diskManager,
		pool:      pool,
		pageTable: make(map[disk.PageID]BufferID),
	}
}

// FetchPage は指定されたページIDのバッファを取得する
// キャッシュにあればそれを返し、なければディスクから読み込む
func (m *BufferPoolManager) FetchPage(pageID disk.PageID) (*Buffer, error) {
	// ページテーブルにあればキャッシュヒット
	if bufferID, ok := m.pageTable[pageID]; ok {
		frame := &m.pool.frames[bufferID]
		frame.UsageCount++
		frame.Buffer.refCount++
		return frame.Buffer, nil
	}

	// キャッシュミス：置換対象を探す
	bufferID, err := m.pool.Evict()
	if err != nil {
		return nil, err
	}

	frame := &m.pool.frames[bufferID]
	evictPageID := frame.Buffer.PageID
	wasValid := frame.Buffer.isValid

	// 古いバッファがdirtyなら書き戻す
	if wasValid && frame.Buffer.IsDirty {
		if err := m.disk.WritePageData(evictPageID, frame.Buffer.Page[:]); err != nil {
			return nil, err
		}
	}

	// 新しいページをディスクから読み込む
	frame.Buffer.PageID = pageID
	frame.Buffer.IsDirty = false
	frame.Buffer.isValid = true
	if err := m.disk.ReadPageData(pageID, frame.Buffer.Page[:]); err != nil {
		return nil, err
	}
	frame.UsageCount = 1
	frame.Buffer.refCount = 1

	// ページテーブルを更新（有効だったバッファのみ削除）
	if wasValid {
		delete(m.pageTable, evictPageID)
	}
	m.pageTable[pageID] = bufferID

	return frame.Buffer, nil
}

// CreatePage は新しいページを作成してバッファを返す
func (m *BufferPoolManager) CreatePage() (*Buffer, error) {
	// 置換対象を探す
	bufferID, err := m.pool.Evict()
	if err != nil {
		return nil, err
	}

	frame := &m.pool.frames[bufferID]
	evictPageID := frame.Buffer.PageID
	wasValid := frame.Buffer.isValid

	// 古いバッファがdirtyなら書き戻す
	if wasValid && frame.Buffer.IsDirty {
		if err := m.disk.WritePageData(evictPageID, frame.Buffer.Page[:]); err != nil {
			return nil, err
		}
	}

	// 新しいページを割り当て
	pageID := m.disk.AllocatePage()

	// バッファを初期化
	frame.Buffer.PageID = pageID
	frame.Buffer.Page = Page{} // ゼロクリア
	frame.Buffer.IsDirty = true // 新規作成なので dirty
	frame.Buffer.isValid = true
	frame.Buffer.refCount = 1
	frame.UsageCount = 1

	// ページテーブルを更新（有効だったバッファのみ削除）
	if wasValid {
		delete(m.pageTable, evictPageID)
	}
	m.pageTable[pageID] = bufferID

	return frame.Buffer, nil
}

// Flush は全てのdirtyページをディスクに書き戻す
func (m *BufferPoolManager) Flush() error {
	for pageID, bufferID := range m.pageTable {
		frame := &m.pool.frames[bufferID]
		if err := m.disk.WritePageData(pageID, frame.Buffer.Page[:]); err != nil {
			return err
		}
		frame.Buffer.IsDirty = false
	}
	return m.disk.Sync()
}
