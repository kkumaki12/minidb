package disk

import (
	"io"
	"os"
)

const PageSize = 4096

type PageID uint64

type DiskManager struct {
	heapFile   *os.File
	nextPageID PageID
}

// NewDiskManager creates a new DiskManager for the given heap file.
func NewDiskManager(heapFile *os.File) (*DiskManager, error) {
	fileInfo, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}
	heapFileSize := fileInfo.Size()
	nextPageID := PageID(heapFileSize / PageSize)

	return &DiskManager{
		heapFile:   heapFile,
		nextPageID: nextPageID,
	}, nil
}

func Open(heapFilePath string) (*DiskManager, error) {
	heapFile, err := os.OpenFile(heapFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return NewDiskManager(heapFile)
}

func (d *DiskManager) ReadPageData(pageID PageID, data []byte) error {
	offset := int64(PageSize * pageID)
	_, err := d.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(d.heapFile, data)
	return err
}

func (d *DiskManager) WritePageData(pageID PageID, data []byte) error {
	offset := int64(PageSize * pageID)
	_, err := d.heapFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = d.heapFile.Write(data)
	return err
}

func (d *DiskManager) AllocatePage() PageID {
	pageID := d.nextPageID
	d.nextPageID++
	return pageID
}

func (d *DiskManager) Sync() error {
	return d.heapFile.Sync()
}
