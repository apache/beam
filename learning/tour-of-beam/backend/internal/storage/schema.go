package storage

type Unit struct {
	Id          string `json:"unitId"`
	Name        string `json:"name"`
	IsCompleted bool   `json:"completed"`
}

type Module struct {
	Id    string `json:"moduleId"`
	Name  string `json:"name"`
	Units []Unit `json:"units"`
}

type ContentTree struct {
	Modules []Module `json:"modules"`
}
