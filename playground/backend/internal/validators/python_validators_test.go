package validators

import "testing"

const (
	pyUnitTestFilePath = "test.py"
	pyUnitTestCode     = "import unittest py code"
	pyCodeFilePath     = "notTest.py"
	pyTestCode         = "py code"
)

func TestCheckIsUnitTestPy(t *testing.T) {
	unitTestValidatorArgs := make([]interface{}, 1)
	unitTestValidatorArgs[0] = pyUnitTestFilePath
	validatorArgs := make([]interface{}, 1)
	validatorArgs[0] = pyCodeFilePath
	argsWithoutRealFile := make([]interface{}, 1)
	argsWithoutRealFile[0] = "fileNotExists.py"
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name:    "check that file is a python unit test",
			args:    args{args: unitTestValidatorArgs},
			want:    true,
			wantErr: false,
		},
		{
			name:    "check that file is not a python unit test",
			args:    args{args: validatorArgs},
			want:    false,
			wantErr: false,
		},
		{
			name:    "error if file not exists",
			args:    args{args: argsWithoutRealFile},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckIsUnitTestPy(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckIsUnitTestPy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckIsUnitTestPy() got = %v, want %v", got, tt.want)
			}
		})
	}
}
