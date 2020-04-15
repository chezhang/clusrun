package main

import (
	"testing"
)

func Test_parseJobIds(t *testing.T) {
	cases := []struct {
		ids               []string
		expectedParsedIds []int32
		expectError       bool
	}{
		{[]string{"*"}, []int32{jobId_all}, false},
		{[]string{"all"}, []int32{jobId_all}, false},
		{[]string{"All"}, []int32{jobId_all}, false},
		{[]string{"ALL"}, []int32{jobId_all}, false},
		{[]string{"~~"}, []int32{-1}, false},
		{[]string{"last"}, []int32{-1}, false},
		{[]string{"x"}, []int32{}, true},
		{[]string{"first"}, []int32{}, true},
		{[]string{"0"}, []int32{}, true},
		{[]string{"1"}, []int32{1}, false},
		{[]string{"2"}, []int32{2}, false},
		{[]string{"-1"}, []int32{-1}, false},
		{[]string{"-2"}, []int32{-2}, false},
		{[]string{"1-5"}, []int32{1, 2, 3, 4, 5}, false},
		{[]string{"5-1"}, []int32{}, false},
		{[]string{"-5-1"}, []int32{}, true},
		{[]string{"-1-5"}, []int32{}, true},
		{[]string{"-1--5"}, []int32{}, true},
		{[]string{"x,*"}, []int32{}, true},
		{[]string{"-1,*"}, []int32{jobId_all, -1}, false},
		{[]string{"1,2,3"}, []int32{1, 2, 3}, false},
		{[]string{"-1,-2,-3"}, []int32{-1, -2, -3}, false},
		{[]string{"-1,2,3"}, []int32{-1, 2, 3}, false},
		{[]string{"-1,0,1"}, []int32{}, true},
		{[]string{"1,2,10-12"}, []int32{1, 2, 10, 11, 12}, false},
		{[]string{"-1,2,10-12"}, []int32{-1, 2, 10, 11, 12}, false},
		{[]string{"1,2,-10-12"}, []int32{}, true},
		{[]string{"1,2,10--12"}, []int32{}, true},
		{[]string{"1,2,-10--12"}, []int32{}, true},
		{[]string{"1,-2,-10--12"}, []int32{}, true},
		{[]string{"1,-2,10-2"}, []int32{-2, 1}, false},
		{[]string{"1,-2,10-2,2-10"}, []int32{-2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, false},
		{[]string{"1-3,5,8-8"}, []int32{1, 2, 3, 5, 8}, false},
		{[]string{"1-5,3,7-8"}, []int32{1, 2, 3, 4, 5, 7, 8}, false},
		{[]string{"1", "2", "3"}, []int32{1, 2, 3}, false},
		{[]string{"-1", "-2", "-3"}, []int32{-1, -2, -3}, false},
		{[]string{"-1", "2", "3"}, []int32{-1, 2, 3}, false},
		{[]string{"-1", "0", "1"}, []int32{}, true},
		{[]string{"1", "2", "10-12"}, []int32{1, 2, 10, 11, 12}, false},
		{[]string{"-1", "2", "10-12"}, []int32{-1, 2, 10, 11, 12}, false},
		{[]string{"1", "2", "-10-12"}, []int32{}, true},
		{[]string{"1", "2", "10--12"}, []int32{}, true},
		{[]string{"1", "2", "-10--12"}, []int32{}, true},
		{[]string{"1", "-2", "-10--12"}, []int32{}, true},
		{[]string{"1", "-2", "10-2"}, []int32{-2, 1}, false},
		{[]string{"1", "-2", "10-2", "2-10"}, []int32{-2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, false},
		{[]string{"1-3", "5", "8-8"}, []int32{1, 2, 3, 5, 8}, false},
		{[]string{"1-5", "3", "7-8"}, []int32{1, 2, 3, 4, 5, 7, 8}, false},
		{[]string{"1-5", "13,3", "7-8"}, []int32{1, 2, 3, 4, 5, 7, 8, 13}, false},
		{[]string{"1-5,2-4", "13,3", "7-8"}, []int32{1, 2, 3, 4, 5, 7, 8, 13}, false},
		{[]string{"1-5,-3", "13,3", "7-8"}, []int32{-3, 1, 2, 3, 4, 5, 7, 8, 13}, false},
		{[]string{"1-5,-3-3", "13,3", "7-8"}, []int32{}, true},
		{[]string{"~*"}, []int32{}, true},
		{[]string{"~all"}, []int32{}, true},
		{[]string{"~All"}, []int32{}, true},
		{[]string{"~ALL"}, []int32{}, true},
		{[]string{"~~~"}, []int32{}, true},
		{[]string{"~last"}, []int32{}, true},
		{[]string{"~x"}, []int32{}, true},
		{[]string{"~first"}, []int32{}, true},
		{[]string{"~0"}, []int32{}, true},
		{[]string{"~1"}, []int32{-1}, false},
		{[]string{"~2"}, []int32{-2}, false},
		{[]string{"~-1"}, []int32{}, true},
		{[]string{"~-2"}, []int32{}, true},
		{[]string{"~1-5"}, []int32{-1, -2, -3, -4, -5}, false},
		{[]string{"~5-1"}, []int32{}, false},
		{[]string{"~-5-1"}, []int32{}, true},
		{[]string{"~-1-5"}, []int32{}, true},
		{[]string{"~-1--5"}, []int32{}, true},
		{[]string{"~1,2,3"}, []int32{-1, 2, 3}, false},
		{[]string{"~1,-2,-3"}, []int32{-1, -2, -3}, false},
		{[]string{"~1,~2,~3"}, []int32{-1, -2, -3}, false},
		{[]string{"~-1,2,3"}, []int32{}, true},
		{[]string{"~1,0,1"}, []int32{}, true},
		{[]string{"~1,2,10-12"}, []int32{-1, 2, 10, 11, 12}, false},
		{[]string{"~1,-2,10-12"}, []int32{-1, -2, 10, 11, 12}, false},
		{[]string{"~1,2,~10-12"}, []int32{-1, 2, -10, -11, -12}, false},
		{[]string{"~1,2,10--12"}, []int32{}, true},
		{[]string{"~1,2,-10--12"}, []int32{}, true},
		{[]string{"~1,-2,-10--12"}, []int32{}, true},
		{[]string{"~1,-2,10-2"}, []int32{-2, -1}, false},
		{[]string{"~1,2,~10-2,~2-10"}, []int32{2, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10}, false},
		{[]string{"~1-3,5,~8-8"}, []int32{-1, -2, -3, 5, -8}, false},
		{[]string{"~1-5,-3,7-8"}, []int32{-1, -2, -3, -4, -5, 7, 8}, false},
		{[]string{"~1-5", "-3", "7-8"}, []int32{-1, -2, -3, -4, -5, 7, 8}, false},
	}

	for _, c := range cases {
		ids, err := parseJobIds(c.ids)
		if err != nil && c.expectError {
			continue
		}
		pass := true
		if err == nil && c.expectError || err != nil && !c.expectError || len(ids) != len(c.expectedParsedIds) {
			pass = false
		} else {
			for _, i := range c.expectedParsedIds {
				if _, ok := ids[i]; !ok {
					pass = false
					break
				}
			}
		}
		if !pass {
			array := make([]int32, 0, len(ids))
			for k := range ids {
				array = append(array, k)
			}
			t.Errorf("\nids=%v\nexpected parsed ids=%v\n  acutal parsed ids=%v\nexpect error=%v\nactual error=%v",
				c.ids, c.expectedParsedIds, array, c.expectError, err)
		}
	}
}
