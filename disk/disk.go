package disk

import "os"

const PageSize = 4096

type PageID uint64

type DiskManager struct {
	heapFile   *os.File
	nextPageID PageID
}
