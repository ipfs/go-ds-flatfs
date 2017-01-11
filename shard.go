package flatfs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type shardId struct {
	version string
	funName string
	param   string
}

func (f shardId) str() string {
	return fmt.Sprintf("/repo/flatfs/shard/v1/%s/%s", f.funName, f.param)
}

func parseShardFunc(str string) shardId {
	str = strings.TrimSpace(str)
	parts := strings.Split(str, "/")
	// ignore prefix for now
	if len(parts) > 3 {
		parts = parts[len(parts)-3:]
	}
	switch len(parts) {
	case 3:
		return shardId{version: parts[0], funName: parts[1], param: parts[2]}
	case 2:
		return shardId{funName: parts[0], param: parts[1]}
	case 1:
		return shardId{funName: parts[0]}
	default: // can only happen for len == 0
		return shardId{}
	}
}

func (f shardId) Func() (ShardFunc, error) {
	if f.version != "" && f.version != "v1" {
		return nil, fmt.Errorf("expected 'v1' for version string got: %s\n", f.version)
	}
	if f.param == "" {
		return nil, fmt.Errorf("'%s' function requires a parameter", f.funName)
	}
	len, err := strconv.Atoi(f.param)
	if err != nil {
		return nil, err
	}
	switch f.funName {
	case "prefix":
		return Prefix(len), nil
	case "suffix":
		return Suffix(len), nil
	case "next-to-last":
		return NextToLast(len), nil
	default:
		return nil, fmt.Errorf("expected 'prefix', 'suffix' or 'next-to-last' got: %s", f.funName)
	}
}

func NormalizeShardFunc(str string) string {
	return parseShardFunc(str).str()
}

func ShardFuncFromString(str string) (ShardFunc, error) {
	id := parseShardFunc(str)
	fun, err := id.Func()
	if err != nil {
		return nil, err
	}
	return fun, nil
}

func ReadShardFunc(dir string) (string, error) {
	buf, err := ioutil.ReadFile(filepath.Join(dir, "SHARDING"))
	if os.IsNotExist(err) {
		return "", ShardingFileMissing
	} else if err != nil {
		return "", err
	}
	return NormalizeShardFunc(string(buf)), nil
}

func WriteShardFunc(dir, str string) error {
	str = NormalizeShardFunc(str)
	_, err := ShardFuncFromString(str)
	if err != nil {
		return err
	}
	file, err := os.Create(filepath.Join(dir, "SHARDING"))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(str)
	if err != nil {
		return err
	}
	_, err = file.WriteString("\n")
	if err != nil {
		return err
	}
	if str == IPFS_DEF_SHARD {
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
