package internal

func (nt NodeType) MarshalText() ([]byte, error) {
	var typ string
	switch nt {
	case NODE_UNIT:
		typ = "unit"
	case NODE_GROUP:
		typ = "group"
	default:
		panic("NodeType not defined")
	}
	return []byte(typ), nil
}
