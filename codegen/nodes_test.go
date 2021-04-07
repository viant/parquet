package codegen

import (
	"fmt"
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

func TestNodes_DefCaseAppendPath(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "slices path",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name:          "A",
						IsSlice:       true,
						ComponentType: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name:          "B",
						IsSlice:       true,
						ComponentType: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A[ind[0]].B",
				"v.A[ind[0]].B[ind[1]].C",
				"v.A[ind[0]].B[ind[1]].C[ind[2]].D",
			},
		},


		{
			description: "mixed path 1 ",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name:          "A",
						IsSlice:       true,
						ComponentType: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A[ind[0]].B.C",
				"v.A[ind[0]].B.C[ind[1]].D",
			},
		},
		{
			description: "mixed path 2 ",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"v.A.B.C",
				"v.A.B.C",
				"v.A.B.C[ind[0]].D",
			},
		},
		{
			description: "mixed path 3 ",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name: "C",
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "E",
					Field: &toolbox.FieldInfo{
						Name:          "E",
						ComponentType: "E",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"v.A.B.C.D",
				"v.A.B.C.D",
				"v.A.B.C.D[ind[0]].E",
			},
		},
	}
	for _, testCase := range testCases {
		testCase.nodes.Init()
		for i, expect := range testCase.expect {
			actual := testCase.nodes.DefCaseAppendPath(i + 1)
			assert.EqualValues(t, expect, actual, testCase.description+fmt.Sprintf("[%v]:%v ", i, expect))
		}
	}

}

func TestNodes_DefCaseAppendValue(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "slices path",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name:          "A",
						IsSlice:       true,
						ComponentType: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name:          "B",
						IsSlice:       true,
						ComponentType: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"A{}",
				"A{B: []B{{}}}",
				"B{C: []C{{}}}",
				"C{D: []D{{}}}",
			},
		},
		{
			description: "mixed path 1 ",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name:          "A",
						IsSlice:       true,
						ComponentType: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"[]A{{}}",
				"A{B: B{}}",
				"C{D: []D{{}}}",
			},
		},
		{
			description: "mixed path 2 ",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"C{}",
				"C{D: []D{{}}}",
			},
		},
		{
			description: "mixed path 3 ",
			nodes: Nodes{
				&Node{
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name: "C",
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "E",
					Field: &toolbox.FieldInfo{
						Name:          "E",
						ComponentType: "E",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"D{}",
				"D{E: []E{{}}}",
			},
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init()

		for i, expect := range testCase.expect {
			actual := testCase.nodes.DefCaseAppendValue(i + 1)
			assert.EqualValues(t, expect, actual, testCase.description+fmt.Sprintf("[%v]:%v ", i, expect))
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
					FieldName: "A",
					Field: &toolbox.FieldInfo{
						Name:          "A",
						IsSlice:       true,
						ComponentType: "A",
					},
				},
				&Node{
					FieldName: "B",
					Field: &toolbox.FieldInfo{
						Name:          "B",
						IsSlice:       true,
						ComponentType: "B",
					},
				},
				&Node{
					FieldName: "C",
					Field: &toolbox.FieldInfo{
						Name:          "C",
						ComponentType: "C",
						IsSlice:       true,
					},
				},
				&Node{
					FieldName: "D",
					Field: &toolbox.FieldInfo{
						Name:          "D",
						ComponentType: "D",
						IsSlice:       true,
					},
				},
			},
			expect: []string{
				"v.A = append(v.A, A{})",
				"v.A = append(v.A, A{B: []B{{}}})",
				"v.A[ind[0]].B = append(v.A[ind[0]].B, B{C: []C{{}}})",
				"v.A[ind[0]].B[ind[1]].C = append(v.A[ind[0]].B[ind[1]].C, C{D: []D{{}}})",
				"TODO add me",
			},
		},
	}



	/*
		switch def {
		case 1:
			x.A = append(x.A, A{})
		case 2:
			x.A = append(x.A, A{B: []B{{}}})
		case 3:
			x.A[ind[0]].B = append(x.A[ind[0]].B, B{C: []C{{}}})
		case 4:
			x.A[ind[0]].B[ind[1]].C = append(x.A[ind[0]].B[ind[1]].C, C{S: []D{{}}})
		case 5:
			x.A[ind[0]].B[ind[1]].C[ind[2]].S = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S, D{E: []E{{}}})
		case 6:
			x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E, E{F: []F{{}}})
		case 7:
			switch rep {
			case 0:
				x.A = []A{{B: []B{{C: []C{{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}}}}}}}
			case 1:
				x.A = append(x.A, A{B: []B{{C: []C{{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}}}}}})
			case 2:
				x.A[ind[0]].B = append(x.A[ind[0]].B, B{C: []C{{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}}}})
			case 3:
				x.A[ind[0]].B[ind[1]].C = append(x.A[ind[0]].B[ind[1]].C, C{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}})
			case 4:
				x.A[ind[0]].B[ind[1]].C[ind[2]].S = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S, D{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}})
			case 5:
				x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E, E{F: []F{{G: []string{vals[nVals]}}}})
			case 6:
				x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F, F{G: []string{vals[nVals]}})
			case 7:
				x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F[ind[5]].G = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F[ind[5]].G, vals[nVals])
			}
			nVals++
		}
	*/

	for _, testCase := range testCases {
		testCase.nodes.Init()
		actuals := testCase.nodes.DefCases(0)
		if !assert.EqualValues(t, len(testCase.expect), len(actuals), testCase.description) {
			continue
		}

		for i, actual := range actuals {
			expect := testCase.expect[i]
			assert.EqualValues(t, expect, actual, testCase.description+" "+toolbox.AsString(i))
		}
	}

}
