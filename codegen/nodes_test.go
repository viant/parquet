package codegen

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"testing"
)

func TestNodes_OwnerPath(t *testing.T) {

	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "slices path",
			nodes: Nodes{
				&Node{
					FieldName: "Root",
					Field: &toolbox.FieldInfo{
						Name:    "Root",
						IsSlice: true,
					},
				},
				&Node{
					FieldName: "Sub",
					Field: &toolbox.FieldInfo{
						Name:    "Sub",
						IsSlice: true,
					},
				},
				&Node{
					FieldName: "Leaf",
					Field: &toolbox.FieldInfo{
						Name:    "Leaf",
						IsSlice: true,
					},
				},
			},
			expect: []string{
				"v", "v0", "v1",
			},
		},

		{
			description: "struct path",
			nodes: Nodes{
				&Node{
					FieldName: "Root",
					Field: &toolbox.FieldInfo{
						Name:    "Root",
						IsSlice: false,
					},
				},
				&Node{
					FieldName: "Sub",
					Field: &toolbox.FieldInfo{
						Name:    "Sub",
						IsSlice: false,
					},
				},
				&Node{
					FieldName: "Leaf",
					Field: &toolbox.FieldInfo{
						Name:    "Leaf",
						IsSlice: false,
					},
				},
			},
			expect: []string{
				"v", "v.Root", "v.Root.Sub",
			},
		},

		{
			description: "struct mix  path 1",
			nodes: Nodes{
				&Node{
					FieldName: "Root",
					Field: &toolbox.FieldInfo{
						Name:    "Root",
						IsSlice: false,
					},
				},
				&Node{
					FieldName: "Sub",
					Field: &toolbox.FieldInfo{
						Name:    "Sub",
						IsSlice: true,
					},
				},
				&Node{
					FieldName: "Leaf",
					Field: &toolbox.FieldInfo{
						Name:    "Leaf",
						IsSlice: true,
					},
				},
			},
			expect: []string{
				"v", "v.Root", "v1",
			},
		},


		{
			description: "struct mix  path 2",
			nodes: Nodes{
				&Node{
					FieldName: "Root",
					Field: &toolbox.FieldInfo{
						Name:    "Root",
						IsSlice: true,
					},
				},
				&Node{
					FieldName: "Sub",
					Field: &toolbox.FieldInfo{
						Name:    "Sub",
						IsSlice: false,
					},
				},
				&Node{
					FieldName: "Leaf",
					Field: &toolbox.FieldInfo{
						Name:    "Leaf",
						IsSlice: true,
					},
				},
			},
			expect: []string{
				"v", "v0", "v0.Sub",
			},
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init()
		for i, expect := range testCase.expect {
			assert.EqualValues(t, expect, testCase.nodes[i].OwnerPath, testCase.description+" "+toolbox.AsString(i))
		}
	}

}

func TestNodes_DefCases(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "slices path",
			nodes: Nodes{
				&Node{
					FieldName: "Root",
					Field: &toolbox.FieldInfo{
						Name:    "Root",
						IsSlice: true,
						ComponentType: "A",
					},
				},
				&Node{
					FieldName: "Sub",
					Field: &toolbox.FieldInfo{
						Name:    "Sub",
						IsSlice: true,
						ComponentType: "C",
					},
				},
				&Node{
					FieldName: "Leaf",
					Field: &toolbox.FieldInfo{
						Name:    "Leaf",
						ComponentType: "C",
						IsSlice: true,
					},
				},
			},
			expect: []string{
				"    v.Root = append(v.Root, A{})",
				" 	 v.Root.Sub = append(v.Root.Sub, C{})",
				" ",
			},
		},
	}
	for _, testCase := range testCases {
		testCase.nodes.Init()
		actuals := testCase.nodes.DefCases()
		if ! assert.EqualValues(t, len(testCase.expect), len(actuals), testCase.description) {
			continue
		}

		for i, actual := range actuals {
			expect := testCase.expect[i]
			assert.EqualValues(t, expect, actual, testCase.description+" "+toolbox.AsString(i))
		}
	}

}
