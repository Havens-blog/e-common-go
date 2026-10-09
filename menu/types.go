// Package menu 定义菜单相关类型，从 ecmdb 解耦后的本地实现
package menu

type Type uint8

func (s Type) ToUint8() uint8 {
	return uint8(s)
}

const (
	DIR    Type = 1
	MENU   Type = 2
	BUTTON Type = 3
)

type Status uint8

func (s Status) ToUint8() uint8 {
	return uint8(s)
}

const (
	ENABLED  Status = 1
	DISABLED Status = 2
)

type Menu struct {
	Id        int64
	Pid       int64
	Path      string
	Name      string
	Sort      int64
	Component string
	Redirect  string
	Status    Status
	Type      Type
	Meta      Meta
	Endpoints []Endpoint
}

type Endpoint struct {
	Path     string
	Method   string
	Resource string
	Desc     string
}

type Meta struct {
	Title       string
	IsHidden    bool
	IsAffix     bool
	IsKeepAlive bool
	Icon        string
	Platforms   []string
}
