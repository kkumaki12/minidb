package disk

import (
	"io"
	"os"
)

// PageSize はディスク上のページサイズ（4KB）
const PageSize = 4096

type PageID uint64

// DiskManager はヒープファイルへのページ単位の読み書きを管理する
type DiskManager struct {
	heapFile   *os.File // ヒープファイルのファイルディスクリプタ
	nextPageID PageID   // 次に割り当てるページID（現在のページ数と同じ）
}

// NewDiskManager は既存のファイルからDiskManagerを作成する
// ファイルサイズから現在のページ数を計算し、次に割り当てるページIDを決定する
func NewDiskManager(heapFile *os.File) (*DiskManager, error) {
	fileInfo, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}
	heapFileSize := fileInfo.Size()
	// ファイルサイズ ÷ ページサイズ = 既存のページ数 = 次のページID
	nextPageID := PageID(heapFileSize / PageSize)

	return &DiskManager{
		heapFile:   heapFile,
		nextPageID: nextPageID,
	}, nil
}

// Open はヒープファイルを開いてDiskManagerを作成する
// ファイルが存在しない場合は新規作成する（O_CREATE）
func Open(heapFilePath string) (*DiskManager, error) {
	// O_RDWR: 読み書き両用, O_CREATE: なければ作成, 0644: rw-r--r--
	heapFile, err := os.OpenFile(heapFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return NewDiskManager(heapFile)
}

// ReadPageData は指定されたページIDのデータを読み込む
// data スライスは呼び出し側で PageSize 分確保しておく必要がある
func (d *DiskManager) ReadPageData(pageID PageID, data []byte) error {
	// ページID × ページサイズ = ファイル内のオフセット位置
	offset := int64(PageSize * pageID)
	_, err := d.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	// io.ReadFull は len(data) バイト読むまでブロックする（EOFならエラー）
	_, err = io.ReadFull(d.heapFile, data)
	return err
}

// WritePageData は指定されたページIDの位置にデータを書き込む
func (d *DiskManager) WritePageData(pageID PageID, data []byte) error {
	offset := int64(PageSize * pageID)
	_, err := d.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = d.heapFile.Write(data)
	return err
}

// AllocatePage は新しいページを割り当ててそのIDを返す
// 実際のディスク書き込みは WritePageData で行う
func (d *DiskManager) AllocatePage() PageID {
	pageID := d.nextPageID
	d.nextPageID++
	return pageID
}

// Sync はバッファの内容をディスクに書き込む（fsync）
// クラッシュ時のデータ損失を防ぐために重要
func (d *DiskManager) Sync() error {
	return d.heapFile.Sync()
}
