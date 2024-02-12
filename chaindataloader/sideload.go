package chaindataloader

import (
	"errors"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"io"

	"github.com/btcsuite/btcd/wire"
)

var (
	ErrUnsupportedSourceType = errors.New("source type is not supported")
)

type ProcessHdrResp struct {
	CurHeight         int32
	NextCheckptHeight int32
}

// Reader is the interface a caller fetches data from.
type Reader interface {
	// EndHeight indicates the height of the last Header in the chaindataloader
	// source.
	EndHeight() uint32

	// StartHeight indicates the height of the first Header in the
	// chaindataloader source.
	StartHeight() uint32

	// HeadersChain returns the bitcoin network the headers in the
	// chaindataloader source belong to.
	HeadersChain() wire.BitcoinNet

	Load(uint32) (*ProcessHdrResp, error)

	SetHeight(uint32) error
}

// SourceType is a type that indicates the encoding format of the
// sideload source.
type SourceType uint8

const (
	// Binary is a SourceType that uses binary number system to encode
	// information.
	Binary SourceType = 0
)

// dataType indicates the type of data stored by the side load source.
type dataType uint8

const (
	// blockHeaders is a data type that indicates the data stored is
	// *wire.BlockHeaders.
	blockHeaders dataType = 0

	filterHeaders dataType = 1
)

type dataSize uint32

// ReaderConfig is a struct that contains configuration details required to
// fetch a Reader.
type ReaderConfig struct {

	// SourceType is the format type of the chaindataloader source.
	SourceType SourceType

	// Reader is the chaindataloader's source.
	Reader io.ReadSeeker
}

type BlkHdrReaderConfig struct {
	ReaderConfig
	ProcessBlkHeader func([]*wire.BlockHeader) (*ProcessHdrResp, error)
}

type FilterHdrReaderConfig struct {
	ReaderConfig
	ProcessCfHeader func([]*chainhash.Hash) (*ProcessHdrResp, error)
	FilterType      wire.FilterType
}

// NewBlockHeaderReader initializes a block header Reader based on the source
// type of the reader config.
func NewBlockHeaderReader(cfg *BlkHdrReaderConfig) (Reader, error) {

	var (
		reader Reader
		err    error
	)

	switch {
	case cfg.SourceType == Binary:
		reader, err = newBinaryBlkHdrChainDataLoader(cfg)
	default:
	}

	return reader, err
}

func NewFilterHeaderReader(cfg *FilterHdrReaderConfig) (Reader, error) {

	var (
		reader Reader
		err    error
	)

	switch {
	case cfg.SourceType == Binary:
		reader, err = newBinaryFilterHdrChainDataLoader(cfg)
	default:
	}

	return reader, err
}
