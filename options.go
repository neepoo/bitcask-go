package bitcast_go

const (
	defaultDataFileSize = 4 << 20 // 4Mib
)

type Options struct {
	// 文件存储目录
	Dir string
	// 单个数据文件最大尺寸
	// 如果logRecord太大，就算新开了一个数据文件也无法容纳，那么新的数据文件就无视这个限制
	MaxSize int64
	// 写文件是否总是Sync
	AlwaysSync bool
}

// NewDefaultOptions 返回默认的配置项
func NewDefaultOptions() *Options {
	return &Options{
		Dir:        "bit_cask_data_dir",
		MaxSize:    defaultDataFileSize,
		AlwaysSync: false,
	}
}

// NewOptions 返回自定义的配置项
func NewOptions(opts []OptionsFunc) *Options {
	res := new(Options)
	for _, opt := range opts {
		opt(res)
	}
	return res
}

type OptionsFunc func(*Options)

func DirOption(dir string) OptionsFunc {
	return func(o *Options) {
		o.Dir = dir
	}
}

func MaxSizeOption(maxSize int64) OptionsFunc {
	return func(o *Options) {
		o.MaxSize = maxSize
	}
}

func AlwaysSyncOption(alwaysSync bool) OptionsFunc {
	return func(o *Options) {
		o.AlwaysSync = alwaysSync
	}
}
