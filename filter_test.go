package parquetx

import (
	"reflect"
	"testing"
)

func Test_intersection(t *testing.T) {

	type args struct {
		aa []int
		bb []int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{name: "simple", args: args{aa: []int{1, 2, 3, 4}, bb: []int{0, 2, 4, 5, 6}}, want: []int{2, 4}},                                              //
		{name: "simple", args: args{aa: []int{1, 2, 3, 4, 123, 234, 544, 665, 756, 876, 964}, bb: []int{0, 2, 4, 5, 6, 665}}, want: []int{2, 4, 665}}, //
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intersection(tt.args.aa, tt.args.bb); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intersection() = %v, want %v", got, tt.want)
			}
		})
	}

}
