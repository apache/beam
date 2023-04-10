package k8s

import k8s "sigs.k8s.io/controller-runtime/pkg/client"

var (
	OperationNoOp   Operation = "none"
	OperationError  Operation = "error"
	OperationCreate Operation = "create"
	OperationUpdate Operation = "update"
)

type Operation string

func keyFrom(obj k8s.Object) k8s.ObjectKey {
	return k8s.ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}
