package codegen

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"strings"
	"testing"
)

func TestNodes_MaxDef(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      int
	}{
		{
			description: "all required - zero def",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, false),
				tNode("C", false, false),
			},
		},
		{
			description: "repeated",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, false),
				tNode("C", true, false),
			},
			expect: 2,
		},
		{
			description: "optional",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", false, true),
			},
			expect: 2,
		},
		{
			description: "optional",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", true, true),
			},
			expect: 2,
		},
	}
	for _, testCase := range testCases {
		testCase.nodes.Init(false)
		assert.EqualValues(t, testCase.expect, testCase.nodes.MaxDef(), testCase.description)
	}
}

func TestNodes_MaxRep(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      int
	}{
		{
			description: "all required - zero def",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, false),
				tNode("C", false, false),
			},
		},
		{
			description: "repeated",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, false),
				tNode("C", true, false),
			},
			expect: 2,
		},
		{
			description: "optional",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", false, true),
			},
			expect: 0,
		},
		{
			description: "optional",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", true, true),
			},
			expect: 1,
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init(false)
		assert.EqualValues(t, testCase.expect, testCase.nodes.MaxRep(), testCase.description)
	}
}

func TestNodes_DefCasePath(t *testing.T) {

	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "root and leaf slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A",
				"v.A",
			},
		},
		{
			description: "slices 1",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A[ind[0]].B",
				"v.A[ind[0]].B[ind[1]].C",
			},
		},


		{
			description: "slices 2",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("E", true, false),
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
			description: "slice at depth -1",
			nodes: Nodes{
				tNode("A", false, true),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", true, false),
				tNode("S", false, true),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A",
				"v.A",
				"v.A",
			},
		},
		{
			description: "root slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, false),
				tNode("D", false, true),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A",
			},
		},
		{
			description: "root slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, false),
				tNode("D", false, true),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A",
			},
		},

		{
			description: "optional",
			nodes: Nodes{
				tNode("A", false, true),
			},
			expect: []string{
				"v.A",
			},
		},

		{
			description: "root and optional leaf",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
			},
			expect: []string{
				"v.A.B",
				"v.A.B",
				"v.A.B",
			},
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init(false)
		if !assert.EqualValues(t, testCase.nodes.MaxDef(), len(testCase.expect), testCase.description) {
			continue
		}
		for i := 1; i < testCase.nodes.MaxDef(); i++ {
			actual := testCase.nodes.DefCasePath(i)
			expect := testCase.expect[i-1]
			assert.EqualValues(t, expect, actual, testCase.description+fmt.Sprintf("[%v]: %v", i, expect))
		}
	}

}

func TestNodes_DefCaseValue(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "root and leaf slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
			},
			expect: []string{
				"ACT{}",
				"ACT{B: &BT{}}",
				"ACT{B: &BT{C: &CT{}}}",
				"ACT{B: &BT{C: &CT{D: &DT{}}}}",
			},
		},
		{
			description: "slices",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
			},
			expect: []string{
				"ACT{}",
				"ACT{B: []BCT{{}}}",
				"BCT{C: []CCT{{}}}",
				"CCT{D: []DCT{{}}}",
			},
		},
		{
			description: "slice at depth -1",
			nodes: Nodes{
				tNode("A", false, true),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", true, false),
				tNode("S", false, true),
			},
			expect: []string{
				"&AT{}",
				"&AT{B: &BT{}}",
				"&AT{B: &BT{C: &CT{}}}",
				"&AT{B: &BT{C: &CT{D: []DCT{{}}}}}",
			},
		},

		{
			description: "root slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, false),
				tNode("D", false, true),
			},
			expect: []string{
				"ACT{}",
				"ACT{B: &BT{}}",
			},
		},


		{
			description: "root slice",
			nodes: Nodes{
				tNode("A:string", false, true),
			},
			expect: []string{
				"pstring(vals[0])",
			},
		},
		{
			description: "optional case values",
			nodes: Nodes{
				tNode("A", false, true),
				tNode("B", false, true),
			},
			expect: []string{
				"&AT{}",
				"&AT{B: &aVal}",
			},
		},
		{
			description: "optional case values",
			nodes: Nodes{
				tNode("A", false, true),
				tNode("B", false, true),
				tNode("C", false, true),
			},
			expect: []string{
				"&AT{}",
				"&AT{B: &BT{}}",
				"&AT{B: &BT{C: &aVal}}",
			},
		},
	}

	size := len(testCases)
	for _, testCase := range testCases[size-1:] {
		testCase.nodes.Init(false)
		for i, expect := range testCase.expect {
			////eee

			actual := testCase.nodes.DefCaseValue(i + 1)
			assert.EqualValues(t, expect, actual, testCase.description+fmt.Sprintf("[%v]: %v", i, expect))
		}
	}
}

func TestNodes_RepCasePath(t *testing.T) {

	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "root and leaf slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
				tNode("S", true, true),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A[ind[0]].B.C.D.S",
			},
		},
		{
			description: "slices",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("S", true, false),
			},
			expect: []string{
				"v.A",
				"v.A",
				"v.A[ind[0]].B",
				"v.A[ind[0]].B[ind[1]].C",
				"v.A[ind[0]].B[ind[1]].C[ind[2]].D",
				"v.A[ind[0]].B[ind[1]].C[ind[2]].D[ind[3]].S",
			},
		},

		{
			description: "slice at depth -1",
			nodes: Nodes{
				tNode("A", false, true),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", true, false),
				tNode("S", false, true),
			},
			expect: []string{
				"v.A",
				"v.A.B.C.D",
			},
		},
		{
			description: "root slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, false),
				tNode("D", false, true),
			},
			expect: []string{
				"v.A",
				"v.A",
			},
		},
		{
			description: "single slice node",
			nodes: Nodes{
				tNode("A", true, false),
			},
			expect: []string{
				"v.A",
				"v.A",
			},
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init(false)

		for i, expect := range testCase.expect {
			actual := testCase.nodes.RepCasePath(i)
			assert.EqualValues(t, expect, actual, testCase.description+fmt.Sprintf("[%v]: %v", i, expect))
		}
	}
}

func TestNodes_RepCaseValue(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		expect      []string
	}{
		{
			description: "root and leaf slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
				tNode("S", true, true),
			},
			expect: []string{
				"[]ACT{{B: &BT{C: &CT{D: &DT{S: []SCT{vals[nVals]}}}}}}",
				"ACT{B: &BT{C: &CT{D: &DT{S: []SCT{vals[nVals]}}}}}",
				"vals[nVals]",
			},
		},
		{
			description: "slices",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("S", true, false),
			},
			expect: []string{
				"[]ACT{{B: []BCT{{C: []CCT{{D: []DCT{{S: []SCT{vals[nVals]}}}}}}}}}",
				"ACT{B: []BCT{{C: []CCT{{D: []DCT{{S: []SCT{vals[nVals]}}}}}}}}",
				"BCT{C: []CCT{{D: []DCT{{S: []SCT{vals[nVals]}}}}}}",
				"CCT{D: []DCT{{S: []SCT{vals[nVals]}}}}",
				"DCT{S: []SCT{vals[nVals]}}",
				"vals[nVals]",
			},
		},

		{
			description: "slice at depth -1",
			nodes: Nodes{
				tNode("A", false, true),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", true, false),
				tNode("S", false, true),
			},
			expect: []string{
				"&AT{B: &BT{C: &CT{D: []DCT{{S: vals[nVals]}}}}}",
				"DCT{S: vals[nVals]}",
			},
		},
		{
			description: "root slice",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, true),
				tNode("C", false, false),
				tNode("D", false, true),
			},
			expect: []string{
				"[]ACT{{B: &BT{C: CT{D: vals[nVals]}}}}",
				"ACT{B: &BT{C: CT{D: vals[nVals]}}}",
			},
		},

		{
			description: "single slice node",
			nodes: Nodes{
				tNode("A", true, false),
			},
			expect: []string{
				"[]ACT{vals[nVals]}",
				"vals[nVals]",
			},
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init(false)
		for i, expect := range testCase.expect {
			actual := testCase.nodes.RepCaseValue(i)
			assert.EqualValues(t, expect, actual, testCase.description+fmt.Sprintf("[%v]: %v", i, expect))
		}
	}
}

func TestNodes_DefRepPos(t *testing.T) {
	var testCases = []struct {
		description string
		nodes       Nodes
		def         int
		expectPos   int
	}{
		{
			description: "none",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, false),
				tNode("C", false, false),
			},
			expectPos: -1,
		},
		{
			description: "repeated 1",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, false),
				tNode("C", true, false),
			},
			def:       1,
			expectPos: 0,
		},
		{
			description: "repeated 2",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, false),
				tNode("C", true, false),
			},
			def:       1,
			expectPos: 0,
		},
		{
			description: "mixed 1",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", false, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("E", true, false),
			},
			def:       4,
			expectPos: 3,
		},
		{description: "mixed 2",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
			},
			def:       1,
			expectPos: 1,
		},
		{
			description: "mixed 3",
			nodes: Nodes{
				tNode("A", false, false),
				tNode("B", false, true),
				tNode("C", false, true),
				tNode("D", false, true),
			},
			def:       2,
			expectPos: 1,
		},

		{
			description: "slices def 2",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("E", true, false),
			},
			def:       2,
			expectPos: 0,
		},
		{
			description: "slices def 3",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("E", true, false),
			},
			def:       3,
			expectPos: 1,
		},
		{
			description: "slices def 4",
			nodes: Nodes{
				tNode("A", true, false),
				tNode("B", true, false),
				tNode("C", true, false),
				tNode("D", true, false),
				tNode("E", true, false),
			},
			def:       4,
			expectPos: 2,
		},
	}

	for _, testCase := range testCases {
		testCase.nodes.Init(false)
		assert.EqualValues(t, testCase.expectPos, testCase.nodes.DefCasePathPos(testCase.def), testCase.description)
	}
}

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
		testCase.nodes.Init(false)
		for i, expect := range testCase.expect {
			assert.EqualValues(t, expect, testCase.nodes[i].OwnerPath, testCase.description+" "+toolbox.AsString(i))
		}
	}
}

//
//func TestNodes_DefCases(t *testing.T) {
//	var testCases = []struct {
//		description string
//		nodes       Nodes
//		expect      []string
//	}{
//		{
//			description: "slices path 0",
//			nodes: Nodes{
//				&Node{
//					FieldName: "A",
//					Field: &toolbox.FieldInfo{
//						Name:          "A",
//						IsSlice:       true,
//						ComponentType: "A",
//					},
//				},
//				&Node{
//					FieldName: "B",
//					Field: &toolbox.FieldInfo{
//						Name:          "B",
//						IsSlice:       true,
//						ComponentType: "B",
//					},
//				},
//				&Node{
//					FieldName: "C",
//					Field: &toolbox.FieldInfo{
//						Name:          "C",
//						ComponentType: "C",
//						IsSlice:       true,
//					},
//				},
//				&Node{
//					FieldName: "D",
//					Field: &toolbox.FieldInfo{
//						Name:          "D",
//						ComponentType: "D",
//						IsSlice:       true,
//					},
//				},
//			},
//			expect: []string{
//				"v.A = append(v.A, A{})",
//				"v.A = append(v.A, A{B: []B{{}}})",
//				"v.A[ind[0]].B = append(v.A[ind[0]].B, B{C: []C{{}}})",
//				"v.A[ind[0]].B[ind[1]].C = append(v.A[ind[0]].B[ind[1]].C, C{D: []D{{}}})",
//				"TODO add me",
//			},
//		},
//	}
//
//	/*
//		switch def {
//		case 1:
//			x.A = append(x.A, A{})
//		case 2:
//			x.A = append(x.A, A{B: []B{{}}})
//		case 3:
//			x.A[ind[0]].B = append(x.A[ind[0]].B, B{C: []C{{}}})
//		case 4:
//			x.A[ind[0]].B[ind[1]].C = append(x.A[ind[0]].B[ind[1]].C, C{S: []D{{}}})
//		case 5:
//			x.A[ind[0]].B[ind[1]].C[ind[2]].S = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S, D{E: []E{{}}})
//		case 6:
//			x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E, E{F: []F{{}}})
//		case 7:
//			switch rep {
//			case 0:
//				x.A = []A{{B: []B{{C: []C{{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}}}}}}}
//			case 1:
//				x.A = append(x.A, A{B: []B{{C: []C{{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}}}}}})
//			case 2:
//				x.A[ind[0]].B = append(x.A[ind[0]].B, B{C: []C{{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}}}})
//			case 3:
//				x.A[ind[0]].B[ind[1]].C = append(x.A[ind[0]].B[ind[1]].C, C{S: []D{{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}}}})
//			case 4:
//				x.A[ind[0]].B[ind[1]].C[ind[2]].S = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S, D{E: []E{{F: []F{{G: []string{vals[nVals]}}}}}})
//			case 5:
//				x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E, E{F: []F{{G: []string{vals[nVals]}}}})
//			case 6:
//				x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F, F{G: []string{vals[nVals]}})
//			case 7:
//				x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F[ind[5]].G = append(x.A[ind[0]].B[ind[1]].C[ind[2]].S[ind[3]].E[ind[4]].F[ind[5]].G, vals[nVals])
//			}
//			nVals++
//		}
//	*/
//
//	for _, testCase := range testCases {
//		testCase.nodes.Init()
//		actuals := testCase.nodes.DefCases(0)
//		if !assert.EqualValues(t, len(testCase.expect), len(actuals), testCase.description) {
//			continue
//		}
//
//		for i, actual := range actuals {
//			expect := testCase.expect[i]
//			assert.EqualValues(t, expect, actual, testCase.description+" "+toolbox.AsString(i))
//		}
//	}
//
//}

//tNode creates a test node
func tNode(name string, slice interface{}, isPointer bool) *Node {
	componentType := name + "CT"
	typeName := name + "T"
	if strings.Contains(name, ":") {
		pair := strings.SplitN(name, ":", 2)
		name = pair[0]
		typeName = pair[1]

	}

	isSlice := false
	switch v := slice.(type) {
	case bool:
		isSlice = v
	case string:
		componentType = v
		isSlice = true

	}
	node := &Node{
		Field: &toolbox.FieldInfo{
			Name:      name,
			TypeName:  typeName,
			IsSlice:   isSlice,
			IsPointer: isPointer,
		},
	}
	if isSlice {
		node.Field.ComponentType = componentType
	}
	return node
}
