package storage

type Iface interface {
	GetContentTree(sdk string, userId *string) ContentTree
	GetUnitContent(unitId string, userId *string) UnitContent
}
