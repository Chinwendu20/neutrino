package chaindataloader

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/tlv"
)

const (
	blockHeaderSize  dataSize = 80
	filterHeaderSize dataSize = 32
)

/*

================================================================================

ENCODING FORMAT
================================================================================

Each file has a header of varint size consisting of,

dataType || startHeight || endHeight || chain

in that order.
*/

// binReader is an internal struct that holds all data the binReader needs
// to fetch headers.
type binReader struct {
	// reader represents the source to be read.
	reader io.ReadSeeker

	// startHeight represents the height of the first header in the file.
	startHeight uint32

	// endHeight represents the height of the last header in the file.
	endHeight uint32

	// offset represents the distance required to read the first header from
	// the file.
	initialOffset uint32

	// chain represents the bitcoin network the headers in the file belong to.
	chain        wire.BitcoinNet
	dataTypeSize uint32
}

type processHeader[T neutrinoHeader] func([]T) (*ProcessHdrResp, error)

type blkHdrBinReader struct {
	*binReader
	processBlkHdr processHeader[*wire.BlockHeader]
}

type filterHdrBinReader struct {
	*binReader
	processCfHdr processHeader[*chainhash.Hash]
}

func (b *blkHdrBinReader) Load(n uint32) (*ProcessHdrResp, error) {
	hdrs, err := readHeaders(n, blkHdrDecoder, b.dataTypeSize, b.reader)

	if err != nil {
		return nil, err
	}

	resp, err := b.processBlkHdr(hdrs)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (b *filterHdrBinReader) Load(n uint32) (*ProcessHdrResp, error) {
	hdrs, err := readHeaders(n, filterHdrDecoder, b.dataTypeSize, b.reader)

	if err != nil {
		return nil, err
	}

	resp, err := b.processCfHdr(hdrs)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func newBinaryBlkHdrChainDataLoader(c *BlkHdrReaderConfig) (*blkHdrBinReader,
	error) {

	scratch := [8]byte{}
	typeOfData, err := tlv.ReadVarInt(c.Reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if dataType(typeOfData) != blockHeaders {
		return nil, fmt.Errorf("data type mismatch: got %v but expected %v",
			dataType(typeOfData), blockHeaders)
	}

	bin, err := newBinaryChainDataLoader(c.Reader)
	if err != nil {
		return nil, err
	}

	bin.dataTypeSize = uint32(blockHeaderSize)

	return &blkHdrBinReader{
		binReader:     bin,
		processBlkHdr: c.ProcessBlkHeader,
	}, nil

}

func newBinaryFilterHdrChainDataLoader(c *FilterHdrReaderConfig) (
	*filterHdrBinReader,
	error) {

	scratch := [8]byte{}
	typeOfData, err := tlv.ReadVarInt(c.Reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if dataType(typeOfData) != filterHeaders {
		return nil, fmt.Errorf("data type mismatch: got %v but expected %v",
			dataType(typeOfData), filterHeaders)
	}

	filterType, err := tlv.ReadVarInt(c.Reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining data type "+
			"of file %w", err)
	}

	if wire.FilterType(filterType) != c.FilterType {
		return nil, fmt.Errorf("filter type mismatch: got %v but "+
			"expected %v", filterType, c.FilterType)
	}
	bin, err := newBinaryChainDataLoader(c.Reader)
	if err != nil {
		return nil, err
	}
	bin.dataTypeSize = uint32(filterHeaderSize)

	return &filterHdrBinReader{
		binReader:    bin,
		processCfHdr: c.ProcessCfHeader,
	}, nil

}

// newBinaryChainDataLoader initializes a Binary Reader.
func newBinaryChainDataLoader(reader io.ReadSeeker) (
	*binReader, error) {

	// Create scratch buffer.
	scratch := [8]byte{}

	// Read start height of block header file.
	start, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining start height "+
			"of file %w", err)
	}

	// Read end height of block header file.
	end, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining end height of file "+
			"%w", err)
	}

	// Read the bitcoin network, the headers in the header file belong to.
	chainChar, err := tlv.ReadVarInt(reader, &scratch)
	if err != nil {
		return nil, fmt.Errorf("error obtaining chain of file %w", err)
	}

	var chain wire.BitcoinNet
	switch {
	case chainChar == 1:
		chain = wire.TestNet3
	case chainChar == 2:
		chain = wire.MainNet
	case chainChar == 3:
		chain = wire.SimNet
	case chainChar == 4:
		chain = wire.TestNet
	default:
		return nil, fmt.Errorf("read unsupported character (%d) for "+
			"network of side-load source file", chainChar)
	}

	// obtain space occupied by metadata as initial offset
	initialOffset, err := reader.Seek(0, io.SeekCurrent)

	if err != nil {
		return nil, fmt.Errorf("unable to determine initial offset: "+
			"%v", err)
	}

	return &binReader{
		reader:        reader,
		startHeight:   uint32(start),
		endHeight:     uint32(end),
		chain:         chain,
		initialOffset: uint32(initialOffset),
	}, nil
}

type neutrinoHeader interface {
	*wire.BlockHeader | *chainhash.Hash
}
type headerDecoder[T neutrinoHeader] func([]byte) (T, error)

func blkHdrDecoder(data []byte) (*wire.BlockHeader, error) {
	var blockHeader wire.BlockHeader

	headerReader := bytes.NewReader(data)

	// Finally, decode the raw bytes into a proper bitcoin header.
	if err := blockHeader.Deserialize(headerReader); err != nil {
		return nil, fmt.Errorf("error deserializing block header: %w",
			err)
	}

	return &blockHeader, nil
}

func filterHdrDecoder(data []byte) (*chainhash.Hash, error) {

	return chainhash.NewHash(data)
}

func readHeaders[T neutrinoHeader](numHeaders uint32,
	decoder headerDecoder[T], dataTypeSize uint32, reader io.ReadSeeker) (
	[]T, error) {

	hdrs := make([]T, numHeaders)
	for i := uint32(0); i < numHeaders; i++ {
		rawData := make([]byte, dataTypeSize)

		if _, err := reader.Read(rawData); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		T, err := decoder(rawData)

		if err != nil {
			return nil, err
		}
		hdrs[i] = T
	}
	return hdrs, nil
}

// EndHeight function returns the height of the last header in the file.
func (b *binReader) EndHeight() uint32 {
	return b.endHeight
}

// StartHeight function returns the height of the first header in the file.
func (b *binReader) StartHeight() uint32 {
	return b.startHeight
}

// HeadersChain function returns the network the headers in the file belong to.
func (b *binReader) HeadersChain() wire.BitcoinNet {
	return b.chain
}

// SetHeight function receives a height which should be the height of the last
// header the caller has. It uses this to set the appropriate offest for
// fetching Headers.
func (b *binReader) SetHeight(height uint32) error {
	if b.startHeight > height || height > b.endHeight {
		return errors.New("unable to set height as file does not " +
			"contain requested height")
	}

	// Compute offset to fetch header at `height` relative to the binReader's
	// header's start height.
	offset := (height - b.startHeight) * b.dataTypeSize

	// Compute total offset to fetch next header *after* header at `height`
	// relative to the reader's start point which contains the reader's
	// metadata as well.
	totalOffset := (offset + b.initialOffset) + b.dataTypeSize

	_, err := b.reader.Seek(int64(totalOffset), io.SeekStart)

	if err != nil {
		return fmt.Errorf("unable to set seek for Reader: %v", err)
	}

	return nil
}
