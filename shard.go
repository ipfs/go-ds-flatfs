package flatfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type ShardIdV1 struct {
	funName string
	param   int
	fun     ShardFunc
}

const PREFIX = "/repo/flatfs/shard/"

func (f *ShardIdV1) String() string {
	return fmt.Sprintf("%sv1/%s/%d", PREFIX, f.funName, f.param)
}

func (f *ShardIdV1) Func() ShardFunc {
	return f.fun
}

func ParseShardFunc(str string) (*ShardIdV1, error) {
	str = strings.TrimSpace(str)
	// ignore prefix for now
	if len(str) == 0 {
		return nil, fmt.Errorf("empty shard identifier")
	}
	if str[0] == '/' {
		trimmed := strings.TrimPrefix(str, PREFIX)
		if str == trimmed { // nothing trimmed
			return nil, fmt.Errorf("invalid prefix in shard identifier: %s", str)
		}
		str = trimmed
	}

	parts := strings.Split(str, "/")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid shard identifier: %s", str)
	}

	version := parts[0]
	if version != "v1" {
		return nil, fmt.Errorf("expected 'v1' for version string got: %s\n", version)
	}

	id := &ShardIdV1{funName: parts[1]}

	param, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invalid parameter: %v", err)
	}
	id.param = param

	switch id.funName {
	case "prefix":
		id.fun = Prefix(param)
	case "suffix":
		id.fun = Suffix(param)
	case "next-to-last":
		id.fun = NextToLast(param)
	default:
		return nil, fmt.Errorf("expected 'prefix', 'suffix' or 'next-to-last' got: %s", id.funName)
	}

	return id, nil
}

func ReadShardFunc(dir string) (*ShardIdV1, error) {
	buf, err := ioutil.ReadFile(filepath.Join(dir, "SHARDING"))
	if os.IsNotExist(err) {
		return nil, ErrShardingFileMissing
	} else if err != nil {
		return nil, err
	}
	return ParseShardFunc(string(buf))
}

func WriteShardFunc(dir string, id *ShardIdV1) error {
	file, err := os.Create(filepath.Join(dir, "SHARDING"))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(id.String())
	if err != nil {
		return err
	}
	_, err = file.WriteString("\n")
	return err
}

func WriteReadme(dir string, id *ShardIdV1) error {
	if id.String() == IPFS_DEF_SHARD {
		err := ioutil.WriteFile(filepath.Join(dir, "_README"), []byte(README_IPFS_DEF_SHARD), 0444)
		if err != nil {
			return err
		}
	}
	return nil
}

func Prefix(prefixLen int) ShardFunc {
	padding := strings.Repeat("_", prefixLen)
	return func(noslash string) string {
		return (noslash + padding)[:prefixLen]
	}
}

func Suffix(suffixLen int) ShardFunc {
	padding := strings.Repeat("_", suffixLen)
	return func(noslash string) string {
		str := padding + noslash
		return str[len(str)-suffixLen:]
	}
}

func NextToLast(suffixLen int) ShardFunc {
	padding := strings.Repeat("_", suffixLen+1)
	return func(noslash string) string {
		str := padding + noslash
		offset := len(str) - suffixLen - 1
		return str[offset : offset+suffixLen]
	}
}
