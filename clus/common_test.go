package main

import (
	"os"
	"reflect"
	"sort"
	"testing"
)

func Test_ParseNodesOrGroups(t *testing.T) {
	cases := []struct {
		str      string
		file     string
		expected []string
	}{
		{
			"item1,item2,item3",
			"",
			[]string{"item1", "item2", "item3"},
		},
		{
			"item1,item2,item3,item3",
			"",
			[]string{"item1", "item2", "item3"},
		},
		{
			"item1, item2, item3",
			"",
			[]string{"item1", "item2", "item3"},
		},
		{
			"",
			`item1
item2
item3`, []string{"item1", "item2", "item3"},
		},
		{
			"",
			`item1,
item2,
item3`, []string{"item1", "item2", "item3"},
		},
		{
			"",
			`item1,item2,item3`,
			[]string{"item1", "item2", "item3"},
		},
		{
			"",
			`item1

item2
 ,
,
item3
`, []string{"item1", "item2", "item3"},
		},
	}

	const file = "Test_ParseNodesOrGroups.input"

	for _, c := range cases {
		f, err := os.Create(file)
		if err != nil {
			t.Errorf("%v", err)
		}
		if _, err := f.WriteString(c.file); err != nil {
			t.Errorf("%v", err)
		}
		if err := f.Close(); err != nil {
			t.Errorf("%v", err)
		}
		items := ParseNodesOrGroups(c.str, file)
		sort.Strings(items)
		sort.Strings(c.expected)
		if !reflect.DeepEqual(items, c.expected) {
			t.Errorf("\nstr=%v\nfile=%v\nexpected items=%v\nactual items=%v", c.str, c.file, c.expected, items)
		}
		if err := os.Remove(file); err != nil {
			t.Errorf("%v", err)
		}
	}
}
